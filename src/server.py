# Importações da biblioteca padrão
import asyncio
import logging
import os
import warnings
import gc
from operator import add
from typing import List, Annotated
from typing_extensions import TypedDict
import io
import base64
import concurrent.futures

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
from langchain_openai.chat_models import ChatOpenAI  # Versão síncrona
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

# =============================================================================
# Executor global para chamadas bloqueantes
# =============================================================================
executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

# =============================================================================
# Variáveis globais para cache
# =============================================================================
# Cache do esquema do banco (obtido do PDF)
CACHED_DB_SCHEMA = None

# Conexão global com o DuckDB (pode ser aprimorada para um pool)
GLOBAL_DUCKDB_CONN = None

def get_duckdb_connection():
    """Retorna uma conexão com o DuckDB. Se já houver uma conexão aberta, ela é reutilizada."""
    global GLOBAL_DUCKDB_CONN
    if GLOBAL_DUCKDB_CONN is None:
        GLOBAL_DUCKDB_CONN = duckdb.connect('dados_empresas.duckdb')
    return GLOBAL_DUCKDB_CONN

# =============================================================================
# Inicialização do modelo de linguagem (síncrono)
# =============================================================================
model = ChatOpenAI(model_name="gpt-4o-mini", temperature=0)

# =============================================================================
# Definir o estado do agente usando TypedDict
# =============================================================================
class AgentState(TypedDict):
    question: str
    table_schemas: str
    metadata: str
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

# =============================================================================
# Funções de extração e processamento do esquema
# =============================================================================
def extract_metadata_from_pdf(pdf_path):
    metadata =   """
    # Novo Layout para os Dados Abertos do CNPJ

## resultados_consulta

- CNPJ_BASICO (VARCHAR) -  Número base de inscrição no CNPJ (oito primeiros dígitos do CNPJ).  \n
 - RAZAO_SOCIAL (VARCHAR) -  Nome empresarial da pessoa jurídica.        \n
 - NATUREZA_JURIDICA (VARCHAR) -  Código da natureza jurídica. \n
 - QUALIFICACAO_RESPONSAVEL (VARCHAR) -  Qualificação da pessoa física responsável pela empresa.\n
 - CAPITAL_SOCIAL (VARCHAR) - Capital social da empresa. \n
 - PORTE_EMPRESA (VARCHAR) - Código do porte da empresa: 00 – Não informado, 01 – Microempresa, 03 – EPP, 05 – Demais.\n
 - ENTE_FEDERATIVO_RESPONSAVEL (VARCHAR) -  Preenchido para órgãos e entidades do grupo de natureza jurídica 1XXX.  \n
  - CNPJ_ORDEM (VARCHAR) - Número do estabelecimento (9º ao 12º dígito do CNPJ). \n
 - CNPJ_DV (VARCHAR) - Dígito verificador do CNPJ (dois últimos dígitos).        \n
 - IDENTIFICADOR_MATRIZ_FILIAL (VARCHAR) - Código: 1 – Matriz, 2 – Filial.     \n
 - NOME_FANTASIA (VARCHAR) -  Nome fantasia da empresa. \n
 - SITUACAO_CADASTRAL (VARCHAR) -  Código da situação cadastral: 01 – Nula, 2 – Ativa, 3 – Suspensa, 4 – Inapta, 08 – Baixada.\n
 - DATA_SITUACAO_CADASTRAL (VARCHAR) - Data do evento da situação cadastral. \n
 - MOTIVO_SITUACAO_CADASTRAL (VARCHAR) - \n
 - NOME_CIDADE_EXTERIOR (VARCHAR) - \n
 - PAIS (VARCHAR) - \n
 - DATA_INICIO_ATIVIDADE (VARCHAR) - \n
 - CNAE_FISCAL_PRINCIPAL (VARCHAR) -  Código da atividade econômica principal.  \n
 - CNAE_FISCAL_SECUNDARIA (VARCHAR) -  Código das atividades econômicas secundárias, separadas por vírgulas.  \n
 - TIPO_LOGRADOURO (VARCHAR) - \n
 - LOGRADOURO (VARCHAR) - \n
 - NUMERO (VARCHAR) - \n
 - COMPLEMENTO (VARCHAR) - \n
 - BAIRRO (VARCHAR) - \n
 - CEP (VARCHAR) - \n
 - UF (VARCHAR) - \n
 - MUNICIPIO (VARCHAR) - \n
 - DDD_1 (VARCHAR) - \n
 - TELEFONE_1 (VARCHAR) - \n
 - DDD_2 (VARCHAR) - \n
 - TELEFONE_2 (VARCHAR) - \n
 - DDD_FAX (VARCHAR) - \n
 - FAX (VARCHAR) - \n
 - CORREIO_ELETRONICO (VARCHAR) - \n
 - SITUACAO_ESPECIAL (VARCHAR) - \n
 - DATA_SITUACAO_ESPECIAL (VARCHAR) - \n
 - CNPJ_BASICO (VARCHAR) - \n
 - IDENTIFICADOR_SOCIO (VARCHAR) -  Código: 1 – Pessoa Jurídica, 2 – Pessoa Física, 3 – Estrangeiro. \n
 - NOME_SOCIO (VARCHAR) - \n
 - CNPJ_CPF_SOCIO (VARCHAR) - \n
 - QUALIFICACAO_SOCIO (VARCHAR) - \n
 - DATA_ENTRADA_SOCIEDADE (VARCHAR) - \n
 - PAIS (VARCHAR) - \n
 - REPRESENTANTE_LEGAL (VARCHAR) - \n
 - NOME_REPRESENTANTE (VARCHAR) - \n
 - QUALIFICACAO_REPRESENTANTE (VARCHAR) - \n
 - FAIXA_ETARIA (VARCHAR) -  Faixa etária do sócio, de 0 (não se aplica) a 9 (mais de 80 anos).  \n
---

## Tabelas de Domínio

- **Países**: Código e nome do país.  
- **Municípios**: Código e nome do município.  
- **Qualificações de Sócios**: Código e nome da qualificação.  
- **Naturezas Jurídicas**: Código e descrição da natureza jurídica.  
- **CNAEs**: Código e nome da atividade econômica.
    """
    
    return metadata

def get_database_schema(db_path, pdf_path):
    """Extrai o esquema do banco a partir dos metadados do PDF e cria a view no DuckDB."""
    global CACHED_DB_SCHEMA
    if CACHED_DB_SCHEMA is not None:
        return CACHED_DB_SCHEMA

    metadata = extract_metadata_from_pdf(pdf_path)
    print(metadata)
    # Utiliza uma conexão temporária para criação da view
    conn = duckdb.connect(db_path)
    conn.execute("""
    CREATE OR REPLACE VIEW resultados_consulta AS
    SELECT 
       e.*, est.*, s.*
    FROM 
        parquet_scan('data/parquet_empresas/*.parquet') AS e
    LEFT JOIN 
        parquet_scan('data/parquet_estabelecimentos/*.parquet') AS est 
        ON e.CNPJ_BASICO = est.CNPJ_BASICO
    LEFT JOIN 
        parquet_scan('data/parquet_socios/*.parquet') AS s 
        ON e.CNPJ_BASICO = s.CNPJ_BASICO
    """)
    gc.collect()
    cursor = conn.cursor()
    schema = "Tabela: resultados_consulta\nColunas:\n"
    cursor.execute("DESCRIBE resultados_consulta")
    columns = cursor.fetchall()
    schema += f"Tabela: resultados_consulta\n"
    schema += "Colunas:\n"
    #for column in columns:
    #    description = next((desc for desc in metadata.get('Campo Descrição', []) if column[0] in desc), '')
    #    schema += f" - {column[0]} ({column[1]}) - {description}\n"
    #schema += '\n'
    cursor.close()
    conn.close()
    CACHED_DB_SCHEMA = schema
    return schema,metadata

# =============================================================================
# Funções dos nós do LangGraph (versões assíncronas com executor)
# =============================================================================
async def search_engineer_node(state: AgentState):
    # O esquema pode ser calculado de forma síncrona (já que está cacheado)
    db_schema,metadata = get_database_schema('dados_empresas.duckdb', '/home/andsil/projetos/chat_empresas/doc/cnpj-metadados (1).pdf')
    state['table_schemas'] = db_schema
    state['metadata'] = metadata
    state['database'] = 'dados_empresas.duckdb'
    return state

async def sql_writer_node(state: AgentState):
    role_prompt = (
        "You are an expert in DuckDB and its SQL syntax. Your task is to write **only** the SQL query that answers the user's question. "
        "The query should:\n"
        "- Use the standard DuckDB SQL syntax in English.\n"
        "- Use the table and column names as defined in the database schema.\n"
        "- Do not include comments, explanations, or any additional text.\n"
        "- Do not use code formatting or markdown.\n"
        "- Return only a valid SQL query.\n"
        "- Always work with `like` and `%` on string fields in the WHERE clause.\n"
        "- In all results, include the RAZAO_SOCIAL ."
        "- Nunca usar a coluna NOME_FANTASIA."
        "- Sempre mantenha a grafia exata das palavras que eu fornecer, sem nenhuma alteração, apenas passe tudo para MAIUSCULA. Se eu escrever 'Vaccinar', responda exatamente com 'VACCINAR' e não altere para 'Vacinar' ou qualquer outra variação."
    )
    instruction = f"Database schema:\n{state['table_schemas']}\n  Database Metadata:\n{state['metadata']} "
    if state['reflect']:
        instruction += f"Consider the following feedback:\n{chr(10).join(state['reflect'])}\n"
    instruction += (
        "Write the SQL query that answers the question below:\n"
        "Convert the question to uppercase.\n"
        "Do not change the spelling of the question.\n"
        f"{state['question']}\n"
    )
    messages = [
        SystemMessage(content=role_prompt),
        HumanMessage(content=instruction)
    ]
    # Encapsula a chamada bloqueante em um executor
    loop = asyncio.get_running_loop()
    response = await loop.run_in_executor(executor, model.invoke, messages)
    state['sql'] = response.content.strip()
    state['revision'] += 1
    return state


async def execute_query_node(state: AgentState):
    conn = get_duckdb_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(state['sql'])
        state['results'] = cursor.fetchall()
    except Exception as e:
        state['results'] = []
        state['error'] = str(e)
    finally:
        cursor.close()
    return state

async def interpret_results_node(state: AgentState):
    role_prompt = (
        "You are an assistant specialized in interpreting SQL query results with DuckDB syntax, "
        "explaining them in natural language. Your task is to analyze the query results and provide a clear and concise answer to the original user's question."
    )
    instruction = (
        f"Original question: {state['question']}\n"
        f"Executed SQL query: {state['sql']}\n"
        f"Query results: {state['results']}\n"
        "Please interpret these results and answer the original question in natural language."
    )
    messages = [
        SystemMessage(content=role_prompt),
        HumanMessage(content=instruction)
    ]
    loop = asyncio.get_running_loop()
    response = await loop.run_in_executor(executor, model.invoke, messages)
    state['interpretation'] = response.content
    return state

# =============================================================================
# Construir o LangGraph com nós assíncronos
# =============================================================================
builder = StateGraph(AgentState)
builder.add_node('search_engineer', search_engineer_node)
builder.add_node('sql_writer', sql_writer_node)
builder.add_node('execute_query', execute_query_node)
builder.add_node('interpret_results', interpret_results_node)

builder.add_edge(START, 'search_engineer')
builder.add_edge('search_engineer', 'sql_writer')
builder.add_edge('sql_writer', 'execute_query')
builder.add_edge('execute_query', 'interpret_results')
builder.add_edge('interpret_results', END)
builder.set_entry_point('search_engineer')

memory = MemorySaver()
graph = builder.compile(checkpointer=memory)

# =============================================================================
# Função para processar uma pergunta usando o grafo (versão assíncrona)
# =============================================================================
async def process_question(question: str) -> AgentState:
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
    async for _ in graph.astream(initial_state, thread):
        pass
    final_state = graph.get_state(thread).values

    # Opcional: Imprimir os resultados no console
    return final_state

# =============================================================================
# Classe do Servidor gRPC
# =============================================================================
class GenAiServiceServicer(genai_pb2_grpc.GenAiServiceServicer):
    """
    Classe que implementa o serviço gRPC para processamento de perguntas via LLMs.
    """
    async def AskQuestion(self, request, context):
        user_question = request.question
        logger.info("Recebida pergunta via gRPC: %s", user_question)
        try:
            final_state = await process_question(user_question)
            resposta_final = final_state['interpretation']
        except Exception as e:
            logger.error("Erro ao processar a solicitação: %s", str(e))
            resposta_final = f"Erro ao processar a solicitação: {str(e)}"
        logger.info("Resposta final enviada ao cliente: %.50s", resposta_final.replace("\n", " ")[:50])
        return genai_pb2.AnswerResponse(answer=resposta_final)

# =============================================================================
# Função principal de execução do servidor
# =============================================================================
async def serve() -> None:
    server = aio.server()
    genai_pb2_grpc.add_GenAiServiceServicer_to_server(GenAiServiceServicer(), server)
    listen_addr = "[::]:50051"
    server.add_insecure_port(listen_addr)
    logger.info("Servidor configurado para escutar em %s", listen_addr)
    try:
        await server.start()
        logger.info("Servidor iniciado com sucesso em %s", listen_addr)
        await server.wait_for_termination()
    except asyncio.CancelledError:
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
            await asyncio.shield(server.stop(grace=5))
            logger.info("Servidor gRPC desligado com sucesso.")
        except asyncio.CancelledError:
            logger.warning("Shutdown interrompido novamente (Ctrl+C duplo?).")

if __name__ == "__main__":
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        logger.info("Interrupção manual do servidor (KeyboardInterrupt).")
