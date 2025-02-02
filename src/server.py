# Importações da biblioteca padrão
import asyncio
import logging
import os
import warnings
from operator import add
import gc
from typing import Dict, List, Annotated
from typing_extensions import TypedDict
import io
import base64

# Importações de bibliotecas de terceiros
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import duckdb
import pymupdf
from dotenv import load_dotenv
from grpc import aio  # API assíncrona do gRPC

# Importações do LangChain e LangGraph
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables.graph import MermaidDrawMethod
from langchain_openai import ChatOpenAI
from langchain_openai.chat_models import ChatOpenAI
from langchain_aws import ChatBedrockConverse
from langchain_core.messages import SystemMessage, HumanMessage

# Importações do LangGraph
from langgraph.graph import StateGraph, END, START
from langgraph.checkpoint.memory import MemorySaver

# Imports do protocolo gRPC
import genai_pb2
import genai_pb2_grpc


# =============================================================================
# Configuração de Logging
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# =============================================================================
# Carrega variáveis de ambiente
# =============================================================================
load_dotenv()


# Definir o modelo de linguagem
model = ChatOpenAI(model_name="gpt-4o-mini", temperature=0)

# Definir o estado do agente usando TypedDict
class AgentState(TypedDict):
    question: str
    table_schemas: str
    database: str
    sql: str
    reflect: Annotated[List[str], add]
    accepted: bool
    revision: int
    max_revision: int
    results: List[tuple]
    interpretation: str
    plot_needed: bool
    plot_html: str
# Função para extrair metadados do PDF
def extract_metadata_from_pdf(pdf_path):
    metadata = {}
    pdf_document = pymupdf.open(pdf_path)
    for page in pdf_document:
        text = page.get_text()
        lines = text.split('\n')
        current_table = None
        for line in lines:
            if "Campo" in line and "Descrição" in line:
                current_table = line.strip()
                metadata[current_table] = []
            elif current_table and line.strip():
                metadata[current_table].append(line.strip())
    return metadata


def get_database_schema(db_path, pdf_path):
    metadata = extract_metadata_from_pdf(pdf_path)
    DB_FILE = 'dados_empresas.duckdb'  # Opcional, se preferir persistência em disco
    conn = duckdb.connect(DB_FILE)  # ou apenas duckdb.connect() para uma conexão em memória
    # Cria uma tabela resultante a partir do join direto dos arquivos Parquet usando parquet_scan()
    conn.execute("""
    CREATE OR REPLACE VIEW resultados_consulta AS
    SELECT 
        e.CNPJ_BASICO, 
        e.RAZAO_SOCIAL, 
        e.NATUREZA_JURIDICA, 
        e.CAPITAL_SOCIAL, 
        e.PORTE_EMPRESA, 
        est.NOME_FANTASIA, 
        est.CNAE_FISCAL_PRINCIPAL, 
        s.NOME_SOCIO, 
        s.CNPJ_CPF_SOCIO, 
        s.QUALIFICACAO_SOCIO
    FROM 
        parquet_scan('data/parquet_empresas/*.parquet') AS e
    LEFT JOIN 
        parquet_scan('data/parquet_estabelecimentos/*.parquet') AS est 
        ON e.CNPJ_BASICO = est.CNPJ_BASICO
    LEFT JOIN 
        parquet_scan('data/parquet_socios/*.parquet') AS s 
        ON e.CNPJ_BASICO = s.CNPJ_BASICO
    """)
    # Coleta de lixo, se necessário
    gc.collect()
    cursor = conn.cursor()
    schema = ''
    cursor.execute(f"DESCRIBE resultados_consulta")
    columns = cursor.fetchall()
    schema += f"Tabela: resultados_consulta\n"
    schema += "Colunas:\n"
    for column in columns:
        description = next((desc for desc in metadata.get('Campo Descrição', []) if column[0] in desc), '')
        schema += f" - {column[0]} ({column[1]}) - {description}\n"
    schema += '\n'
    conn.close()
    return schema

# Definir os nós para o LangGraph
def search_engineer_node(state: AgentState):
    db_schema = get_database_schema('dados_empresas.duckdb', '/home/andsil/projetos/chat_empresas/doc/cnpj-metadados (1).pdf')
    state['table_schemas'] = db_schema
    state['database'] = 'dados_empresas.duckdb'
    return state

def sql_writer_node(state: AgentState):
    role_prompt = """
You are an expert in DuckDB and its SQL syntax. Your task is to write **only** the SQL query that answers the user's question. The query should:

- Use the standard DuckDB SQL syntax in English.
- Use the table and column names as defined in the database schema.
- Do not include comments, explanations, or any additional text.
- Do not use code formatting or markdown.
- Return only a valid SQL query.
- Always work with `like` and `%` on string fields in the WHERE clause.
- In all results, include the LEGAL NAME.
"""
    instruction = f"Database schema:\n{state['table_schemas']}\n"
    if len(state['reflect']) > 0:
        instruction += f"Consider the following feedback:\n{chr(10).join(state['reflect'])}\n"
    instruction += f"""Write the SQL query that answers the question below:\n
                    Convert the question to uppercase. \n
                    Do not change the spelling of the question. \n
                    {state['question']}\n"""
    messages = [
        SystemMessage(content=role_prompt),
        HumanMessage(content=instruction)
    ]
    response = model.invoke(messages)
    state['sql'] = response.content.strip()
    state['revision'] += 1
    return state


def qa_engineer_node(state: AgentState):
    role_prompt = """
You are a QA engineer specialized in the DuckDB relational database and its SQL syntax. Your task is to check whether the provided SQL query correctly answers the user's question.
"""
    instruction = f"Based on the following database schema:\n{state['table_schemas']}\n"
    instruction += f"And the following SQL query:\n{state['sql']}\n"
    instruction += f"Check if the SQL query can complete the task: {state['question']}\n"
    instruction += "Respond 'ACCEPTED' if it is correct or 'REJECTED' if it is not.\n"
    messages = [
        SystemMessage(content=role_prompt),
        HumanMessage(content=instruction)
    ]
    response = model.invoke(messages)
    state['accepted'] = 'ACCEPTED' in response.content.upper()
    return state


def chief_dba_node(state: AgentState):
    role_prompt = """
You are an experienced DBA, an expert in DuckDB. Your task is to provide detailed feedback to improve the provided SQL query.
"""
    instruction = f"Based on the following database schema:\n{state['table_schemas']}\n"
    instruction += f"And the following SQL query:\n{state['sql']}\n"
    instruction += f"Please provide useful and detailed recommendations to help improve the SQL query for the task: {state['question']}\n"
    messages = [
        SystemMessage(content=role_prompt),
        HumanMessage(content=instruction)
    ]
    response = model.invoke(messages)
    state['reflect'].append(response.content)
    return state


def execute_query_node(state: AgentState):
    conn = duckdb.connect(state['database'])
    cursor = conn.cursor()
    try:
        cursor.execute(state['sql'])
        state['results'] = cursor.fetchall()
    except Exception as e:
        state['results'] = []
        state['error'] = str(e)
    finally:
        cursor.close()
        conn.close()
    return state

def interpret_results_node(state: AgentState):
    role_prompt = """
You are an assistant specialized in interpreting SQL query results with DuckDB syntax, explaining them in natural language.
Your task is to analyze the query results and provide a clear and concise answer to the original user's question.
"""
    instruction = f"Original question: {state['question']}\n"
    instruction += f"Executed SQL query: {state['sql']}\n"
    instruction += f"Query results: {state['results']}\n"
    instruction += "Please interpret these results and answer the original question in natural language."
    messages = [
        SystemMessage(content=role_prompt),
        HumanMessage(content=instruction)
    ]
    response = model.invoke(messages)
    state['interpretation'] = response.content
    # Decide if a chart is needed based on the interpretation
    return state

# Construir o LangGraph
builder = StateGraph(AgentState)

# Adicionar nós ao grafo
builder.add_node('search_engineer', search_engineer_node)
builder.add_node('sql_writer', sql_writer_node)
builder.add_node('qa_engineer', qa_engineer_node)
builder.add_node('chief_dba', chief_dba_node)
builder.add_node('execute_query', execute_query_node)
builder.add_node('interpret_results', interpret_results_node)

# Definir as arestas entre os nós
builder.add_edge(START, 'search_engineer')
builder.add_edge('search_engineer', 'sql_writer')
builder.add_edge('sql_writer', 'qa_engineer')

# Aresta condicional do qa_engineer
builder.add_conditional_edges(
    'qa_engineer',
    lambda state: 'execute_query' if state['accepted'] or state['revision'] >= state['max_revision'] else 'chief_dba',
    {'execute_query': 'execute_query', 'chief_dba': 'chief_dba'}
)

builder.add_edge('chief_dba', 'sql_writer')
builder.add_edge('execute_query', 'interpret_results')
builder.add_edge('interpret_results', END)


# Definir o ponto de entrada
builder.set_entry_point('search_engineer')

# Compilar o grafo com um checkpointer
memory = MemorySaver()
graph = builder.compile(checkpointer=memory)

# Função para processar uma pergunta usando o grafo
def process_question(question):
    initial_state = {
        'question': question,
        'table_schemas': '',
        'database': '',
        'sql': '',
        'reflect': [],
        'accepted': False,
        'revision': 0,
        'max_revision': 2,
        'results': [],
        'interpretation': '',
        'plot_needed': False,
        'plot_html': ''
    }
    thread = {'configurable': {'thread_id': '1'}}
    for s in graph.stream(initial_state, thread):
        pass
    # Obter o estado final
    final_state = graph.get_state(thread).values

    # Imprimir os resultados
    print('Consulta SQL Gerada:\n', final_state['sql'])
    print('\nResultados da Consulta:')
    if 'results' in final_state:
        for result in final_state['results']:
            print(result)
    else:
        print('Nenhum resultado')
    print('\nInterpretação:')
    print(final_state['interpretation'])
    return final_state
    
# =============================================================================
# Classe do Servidor gRPC
# =============================================================================
class GenAiServiceServicer(genai_pb2_grpc.GenAiServiceServicer):
    """
    Classe que implementa o serviço gRPC para processamento de perguntas via LLMs.
    """

    async def AskQuestion(self, request, context):
        """
        Método gRPC que recebe a pergunta e retorna a resposta do Agente de forma assíncrona.

        Args:
            request (genai_pb2.QuestionRequest): Objeto contendo a pergunta do usuário.
            context (grpc.aio.ServicerContext): Contexto de execução do gRPC.

        Returns:
            genai_pb2.AnswerResponse: Resposta a ser enviada ao cliente.
        """
        user_question = request.question
        logger.info("Recebida pergunta via gRPC: %s", user_question)

        try:
            response_text = process_question(user_question)
            resposta_final = response_text['interpretation']
        except Exception as e:
            logger.error("Erro ao processar a solicitação: %s", str(e))
            resposta_final = f"Erro ao processar a solicitação: {str(e)}"

        logger.info("Resposta final enviada ao cliente: %.50s",
                    resposta_final.replace("\n", " ")[:50])
        return genai_pb2.AnswerResponse(answer=resposta_final)


# =============================================================================
# Função principal de execução do servidor
# =============================================================================
async def serve() -> None:
    """
    Inicializa e executa o servidor gRPC de forma assíncrona, 
    lidando com Ctrl+C e interrompendo o loop corretamente.
    """
    server = aio.server()
    genai_pb2_grpc.add_GenAiServiceServicer_to_server(
        GenAiServiceServicer(), server
    )

    listen_addr = "[::]:50051"
    server.add_insecure_port(listen_addr)

    logger.info("Servidor configurado para escutar em %s", listen_addr)

    try:
        # Inicia o servidor
        await server.start()
        logger.info("Servidor iniciado com sucesso em %s", listen_addr)

        # Aguarda até que o servidor seja encerrado (p. ex., sinal de interrupção)
        await server.wait_for_termination()

    except asyncio.CancelledError:
        # Captura quando o loop do asyncio foi cancelado (por ex. Ctrl+C)
        logger.info("Serviço gRPC cancelado. Iniciando desligamento...")

    except OSError as e:
        logger.error("Falha ao iniciar o servidor: %s", str(e))
        raise

    except Exception as e:
        logger.error("Erro inesperado na execução do servidor: %s", str(e))
        raise

    finally:
        logger.info("Desligando servidor gRPC...")
        try:
            # Protege a chamada de shutdown contra cancelamentos
            await asyncio.shield(server.stop(grace=5))
            logger.info("Servidor gRPC desligado com sucesso.")
        except asyncio.CancelledError:
            logger.warning("Shutdown interrompido novamente (Ctrl+C duplo?).")
            # Não re-levanta para evitar traceback


if __name__ == "__main__":
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        logger.info("Interrupção manual do servidor (KeyboardInterrupt).")
