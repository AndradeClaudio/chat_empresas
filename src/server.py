# =============================================================================
# Importações da Biblioteca Padrão
# =============================================================================
import asyncio
import logging
import os
import gc  # Import para coletor de lixo
from operator import add
from typing import List, Annotated
from typing_extensions import TypedDict
import concurrent.futures
import queue
from contextlib import contextmanager

# =============================================================================
# Importações de Bibliotecas de Terceiros
# =============================================================================
import duckdb
from dotenv import load_dotenv
from grpc import aio
import psutil

# =============================================================================
# Importações do LangChain e LangGraph
# =============================================================================
from langchain_openai.chat_models import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage
from langgraph.graph import StateGraph, END, START
from langgraph.checkpoint.memory import MemorySaver

# =============================================================================
# Imports do Protocolo gRPC
# =============================================================================
import genai_pb2
import genai_pb2_grpc

# =============================================================================
# Configuração de Logging e Variáveis de Ambiente
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)
load_dotenv()

# =============================================================================
# Constantes para monitoramento de memória
# =============================================================================
# Define o limite de memória em percentual (ex.: 80%)
MEMORY_THRESHOLD_PERCENT = 80

# =============================================================================
# Função para liberar memória (chama o coletor de lixo)
# =============================================================================
def liberar_memoria():
    gc.collect()
    logger.info("Memória liberada (gc.collect() chamado).")

# =============================================================================
# Executor Global para Chamadas Bloqueantes
# =============================================================================
executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

# =============================================================================
# Variáveis Globais para Cache
# =============================================================================
CACHED_DB_SCHEMA = None  # Cache do esquema do banco (obtido do PDF)
SQL_CACHE = {}           # Cache para resultados de queries

# =============================================================================
# Implementação de um Pool de Conexões para o DuckDB
# =============================================================================
class DuckDBConnectionPool:
    def __init__(self, db_path: str, max_connections: int = 4):
        self.db_path = db_path
        self.pool = queue.Queue(max_connections)
        for _ in range(max_connections):
            conn = duckdb.connect(db_path)
            self.pool.put(conn)

    @contextmanager
    def connection(self):
        conn = self.pool.get()
        try:
            yield conn
        finally:
            self.pool.put(conn)

duckdb_pool = DuckDBConnectionPool('dados_empresas.duckdb', max_connections=4)

# =============================================================================
# Definição do Estado do Agente
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
    needs_human_intervention: bool  # Sinaliza se intervenção humana é necessária

# =============================================================================
# Funções de Extração e Processamento do Esquema
# =============================================================================
def extract_metadata_from_pdf(pdf_path: str) -> str:
    metadata = """
Dados Abertos do CNPJ

Tabela: resultados_consulta
- CNPJ_BASICO (VARCHAR): Número base (8 dígitos).
- RAZAO_SOCIAL (VARCHAR): Nome empresarial.
- NATUREZA_JURIDICA (VARCHAR): Código da natureza jurídica.
- QUALIFICACAO_RESPONSAVEL (VARCHAR): Qualificação do responsável.
- CAPITAL_SOCIAL (VARCHAR): Capital social.
- PORTE_EMPRESA (VARCHAR): Código do porte: 00 – Não informado, 01 – Microempresa, 03 – EPP, 05 – Demais.
- ENTE_FEDERATIVO_RESPONSAVEL (VARCHAR): Órgão responsável.
- CNPJ_ORDEM (VARCHAR): Número do estabelecimento.
- CNPJ_DV (VARCHAR): Dígito verificador.
- IDENTIFICADOR_MATRIZ_FILIAL (VARCHAR): 1 – Matriz, 2 – Filial.
- NOME_FANTASIA (VARCHAR): Nome fantasia.
- SITUACAO_CADASTRAL (VARCHAR): Código situação: 01 – Nula, 02 – Ativa, 03 – Suspensa, 04 – Inapta, 08 – Baixada.
- DATA_SITUACAO_CADASTRAL (VARCHAR): Data do evento.
- MOTIVO_SITUACAO_CADASTRAL (VARCHAR): -
- NOME_CIDADE_EXTERIOR (VARCHAR): -
- PAIS (VARCHAR): -
- DATA_INICIO_ATIVIDADE (VARCHAR): - DATA QUE A EMPRESA INICIOU SUAS ATIVIDADES
- CNAE_FISCAL_PRINCIPAL (VARCHAR): Código atividade principal.
- CNAE_FISCAL_SECUNDARIA (VARCHAR): Códigos secundários (vírgula separada).
- TIPO_LOGRADOURO (VARCHAR): -
- LOGRADOURO (VARCHAR): -
- NUMERO (VARCHAR): -
- COMPLEMENTO (VARCHAR): -
- BAIRRO (VARCHAR): -
- CEP (VARCHAR): -
- UF (VARCHAR): -
- MUNICIPIO (VARCHAR): - CÓDIGO DO MUNICIPIO
- DDD_1 (VARCHAR): -
- TELEFONE_1 (VARCHAR): -
- DDD_2 (VARCHAR): -
- TELEFONE_2 (VARCHAR): -
- DDD_FAX (VARCHAR): -
- FAX (VARCHAR): -
- CORREIO_ELETRONICO (VARCHAR): -
- SITUACAO_ESPECIAL (VARCHAR): -
- DATA_SITUACAO_ESPECIAL (VARCHAR): -
- IDENTIFICADOR_SOCIO (VARCHAR): 1 – Pessoa Jurídica, 2 – Pessoa Física, 3 – Estrangeiro.
- NOME_SOCIO (VARCHAR): -
- CNPJ_CPF_SOCIO (VARCHAR): -
- QUALIFICACAO_SOCIO (VARCHAR): -
- DATA_ENTRADA_SOCIEDADE (VARCHAR): -
- REPRESENTANTE_LEGAL (VARCHAR): -
- NOME_REPRESENTANTE (VARCHAR): -
- QUALIFICACAO_REPRESENTANTE (VARCHAR): -
- FAIXA_ETARIA (VARCHAR): Faixa etária (0 a 9).
- NOME_MUNICIPIO - NOME DO MUNICIPIO

Domínio:
- Países: Código e nome.
- Qualificações de Sócios: Código e nome.
- Naturezas Jurídicas: Código e descrição.
- CNAEs: Código e nome.
    """
    return metadata

def get_database_schema(db_path: str, pdf_path: str):
    global CACHED_DB_SCHEMA
    if CACHED_DB_SCHEMA is not None:
        return CACHED_DB_SCHEMA, extract_metadata_from_pdf(pdf_path)

    metadata = extract_metadata_from_pdf(pdf_path)
    with duckdb_pool.connection() as conn:
        conn.execute("PRAGMA threads=4")
        tables = conn.execute("SHOW TABLES").fetchall()
        if ('resultados_consulta',) not in tables:
            logger.info("Materializando a tabela resultados_consulta...")
            conn.execute("""
            CREATE VIEW resultados_consulta AS
            SELECT 
               e.CNPJ_BASICO
                , e.RAZAO_SOCIAL
                , e.NATUREZA_JURIDICA
                , e.QUALIFICACAO_RESPONSAVEL
                , e.CAPITAL_SOCIAL
                , e.PORTE_EMPRESA
                , e.ENTE_FEDERATIVO_RESPONSAVEL
                , est.CNPJ_ORDEM
                , est.CNPJ_DV
                , est.IDENTIFICADOR_MATRIZ_FILIAL
                , est.NOME_FANTASIA
                , est.SITUACAO_CADASTRAL
                , est.DATA_SITUACAO_CADASTRAL
                , est.MOTIVO_SITUACAO_CADASTRAL
                , est.NOME_CIDADE_EXTERIOR
                , est.PAIS
                , est.DATA_INICIO_ATIVIDADE
                , est.CNAE_FISCAL_PRINCIPAL
                , est.CNAE_FISCAL_SECUNDARIA
                , est.TIPO_LOGRADOURO
                , est.LOGRADOURO
                , est.NUMERO
                , est.COMPLEMENTO
                , est.BAIRRO
                , est.CEP
                , est.UF
                , est.DDD_1
                , est.TELEFONE_1
                , est.DDD_2
                , est.TELEFONE_2
                , est.DDD_FAX
                , est.FAX
                , est.CORREIO_ELETRONICO
                , est.SITUACAO_ESPECIAL
                , est.DATA_SITUACAO_ESPECIAL
                , s.IDENTIFICADOR_SOCIO
                , s.NOME_SOCIO
                , s.CNPJ_CPF_SOCIO
                , s.QUALIFICACAO_SOCIO
                , s.DATA_ENTRADA_SOCIEDADE
                , s.PAIS
                , s.REPRESENTANTE_LEGAL
                , s.NOME_REPRESENTANTE
                , s.QUALIFICACAO_REPRESENTANTE
                , s.FAIXA_ETARIA
                , m.NOME_MUNICIPIO
            FROM 
                parquet_scan('data/parquet_empresas/*.parquet') AS e
            LEFT JOIN 
                parquet_scan('data/parquet_estabelecimentos/*.parquet') AS est 
                ON e.CNPJ_BASICO = est.CNPJ_BASICO
            LEFT JOIN 
                parquet_scan('data/parquet_socios/*.parquet') AS s 
                ON e.CNPJ_BASICO = s.CNPJ_BASICO
            LEFT JOIN 
                parquet_scan('data/parquet_municipios/*.parquet') AS m 
                ON est.MUNICIPIO = m.CODIGO_MUNICIPIO
            """)
            #conn.execute("ANALYZE resultados_consulta;")
        schema = "Tabela: resultados_consulta\nColunas:\n"
        columns = conn.execute("DESCRIBE resultados_consulta").fetchall()
        for column in columns:
            schema += f" - {column[0]} ({column[1]})\n"
    CACHED_DB_SCHEMA = schema
    liberar_memoria()  # Libera memória após processar o esquema
    return schema, metadata

# =============================================================================
# Definição dos Nós do LangGraph (Assíncronos)
# =============================================================================
async def search_engineer_node(state: AgentState):
    db_schema, metadata = get_database_schema(
        'dados_empresas.duckdb',
        '/home/andsil/projetos/chat_empresas/doc/cnpj-metadados (1).pdf'
    )
    state['table_schemas'] = db_schema
    state['metadata'] = metadata
    state['database'] = 'dados_empresas.duckdb'
    liberar_memoria()  # Libera memória após atualizar o estado com esquema e metadados
    return state

async def sql_writer_node(state: AgentState):
    role_prompt = (
        "You are an expert in DuckDB SQL. Your task is to produce a raw SQL query that answers the user's question. "
        "Ensure that the query uses standard DuckDB SQL syntax. In the WHERE clause, for any condition comparing string fields, "
        "you must use the LIKE operator with '%' wildcards and, if required, convert literal strings to uppercase. "
        "CRUCIAL: Do not modify any literal text provided by the user—preserve every character exactly as given. "
        "IMPORTANT: If the query does not require aggregate functions (e.g., COUNT, SUM, AVG), include the column RAZAO_SOCIAL in the SELECT clause; "
        "if the query requires aggregate functions, do not include RAZAO_SOCIAL. "
        "Return only the plain text SQL query without any markdown formatting or extra commentary."
        "Always use DISTINCT in query."
    )
    instruction = (
        f"Schema:\n{state['table_schemas']}\n"
        f"Metadata:\n{state['metadata']}\n"
        "Write the SQL query for the following question (the question is provided exactly as entered, in uppercase):\n"
        f"{state['question']}\n"
    )
    messages = [
        SystemMessage(content=role_prompt),
        HumanMessage(content=instruction)
    ]
    loop = asyncio.get_running_loop()
    response = await loop.run_in_executor(executor, model.invoke, messages)
    state['sql'] = response.content.strip()
    liberar_memoria()  # Libera memória após gerar a query SQL
    return state

async def execute_query_node(state: AgentState):
    # Verifica o consumo de memória atual antes de executar a query
    mem = psutil.virtual_memory()
    if mem.percent >= MEMORY_THRESHOLD_PERCENT:
        state['error'] = (
            f"Alto consumo de memória detectado ({mem.percent:.2f}%). "
            "Consulta abortada para preservar a estabilidade do servidor."
        )
        state['results'] = []
        liberar_memoria()  # Libera memória caso a consulta não seja executada
        return state

    with duckdb_pool.connection() as conn:
        conn.execute("PRAGMA threads=4")
        cursor = conn.cursor()
        try:
            if state['sql'] in SQL_CACHE:
                logger.info("Usando cache para a query.")
                state['results'] = SQL_CACHE[state['sql']]
            else:
                cursor.execute(state['sql'])
                results = cursor.fetchall()
                SQL_CACHE[state['sql']] = results
                state['results'] = results
        except Exception as e:
            state['results'] = []
            state['error'] = str(e)
            logger.error("Erro na query: %s", str(e))
        finally:
            cursor.close()
    liberar_memoria()  # Libera memória após execução da query
    return state

async def interpret_results_node(state: AgentState):
    role_prompt = (
        "You are an assistant specialized in interpreting SQL query results with DuckDB syntax, "
        "explaining them in natural language. Your task is to analyze the query results provided below and answer the original user's question with clarity and precision. "
        "If the query results contain records, incorporate the data into your answer. "
        "If the query results are empty (i.e., no records were returned), explicitly state that no data were found. "
        "If you detect any ambiguity, imprecision, or uncertainty in the query results, append a note like '[DÚVIDA: A resposta pode não estar completa ou precisa]' to your answer."
    )
    instruction = (
        f"Question: {state['question']}\n"
        f"SQL: {state['sql']}\n"
        f"Results: {state['results']}\n"
        "Based on these results, provide a clear, concise, and accurate answer to the user's question. "
        "Ensure that if data are present, your answer reflects them; if no data are returned, clearly state that no data were found."
    )
    messages = [
        SystemMessage(content=role_prompt),
        HumanMessage(content=instruction)
    ]
    loop = asyncio.get_running_loop()
    response = await loop.run_in_executor(executor, model.invoke, messages)
    state['interpretation'] = response.content
    liberar_memoria()  # Libera memória após interpretar os resultados
    return state

# Função simulada para obter feedback humano de forma assíncrona.
async def get_human_input(question: str) -> str:
    logger.info("Aguardando resposta humana para: %s", question)
    await asyncio.sleep(1)
    liberar_memoria()  # Libera memória após aguardar feedback
    return "Esclareça quais dados adicionais você deseja."

# -----------------------------------------------------------------------------
# Nó de intervenção humana (atualizado para perguntar ao usuário)
# -----------------------------------------------------------------------------
async def human_intervention_node(state: AgentState):
    uncertain_keywords = ["DÚVIDA", "INCERTO", "POSSÍVEL ERRO", "REVER", "VERIFICAR"]
    if any(keyword in state['interpretation'].upper() for keyword in uncertain_keywords) or not state['results']:
        clarifying_question = (
            "Detectamos ambiguidade na resposta. "
            "Por favor, informe qual parte da resposta está pouco clara ou quais detalhes adicionais você precisa."
        )
        human_feedback = await get_human_input(clarifying_question)
        state['reflect'].append(human_feedback)
        state['interpretation'] += f"\n\n[CLARIFICATION RECEIVED: {human_feedback}]"
        state['needs_human_intervention'] = True
    else:
        state['needs_human_intervention'] = False
    liberar_memoria()  # Libera memória após intervenção humana
    return state

# =============================================================================
# Tarefa para monitoramento de memória
# =============================================================================
async def monitor_memory():
    while True:
        mem = psutil.virtual_memory()
        if mem.percent >= MEMORY_THRESHOLD_PERCENT:
            logger.warning("Alto consumo de memória detectado: %.2f%%", mem.percent)
        await asyncio.sleep(1)

# =============================================================================
# Inicialização do Modelo de Linguagem com Parâmetro de Tokens
# =============================================================================
model = ChatOpenAI(model_name="gpt-4o-mini", temperature=0, max_tokens=150)

# =============================================================================
# Construção do LangGraph com os Nós Assíncronos
# =============================================================================
builder = StateGraph(AgentState)
builder.add_node('search_engineer', search_engineer_node)
builder.add_node('sql_writer', sql_writer_node)
builder.add_node('execute_query', execute_query_node)
builder.add_node('interpret_results', interpret_results_node)
builder.add_node('human_intervention', human_intervention_node)

builder.add_edge(START, 'search_engineer')
builder.add_edge('search_engineer', 'sql_writer')
builder.add_edge('sql_writer', 'execute_query')
builder.add_edge('execute_query', 'interpret_results')
builder.add_edge('interpret_results', 'human_intervention')
builder.add_edge('human_intervention', END)
builder.set_entry_point('search_engineer')

memory = MemorySaver()
graph = builder.compile(checkpointer=memory)

# =============================================================================
# Função para Processar uma Pergunta Usando o Grafo (Assíncrona)
# =============================================================================
async def process_question(question: str) -> AgentState:
    initial_state = {
        'question': question,
        'table_schemas': '',
        'metadata': '',
        'database': '',
        'sql': '',
        'reflect': [],
        'accepted': False,
        'revision': 0,
        'max_revision': 2,
        'results': [],
        'interpretation': '',
        'plot_needed': False,
        'plot_html': '',
        'needs_human_intervention': False
    }
    thread = {'configurable': {'thread_id': '1'}}
    async for _ in graph.astream(initial_state, thread):
        pass
    final_state = graph.get_state(thread).values
    liberar_memoria()  # Libera memória após o processamento da pergunta
    return final_state

# =============================================================================
# Classe do Servidor gRPC
# =============================================================================
class GenAiServiceServicer(genai_pb2_grpc.GenAiServiceServicer):
    async def AskQuestion(self, request, context):
        user_question = request.question
        logger.info("Pergunta via gRPC: %s", user_question)
        try:
            final_state = await process_question(user_question)
            resposta_final = final_state['interpretation']
        except Exception as e:
            logger.error("Erro: %s", str(e))
            resposta_final = f"Erro: {str(e)}"
        liberar_memoria()  # Libera memória após processar a pergunta via gRPC
        logger.info("Resposta enviada: %.50s", resposta_final.replace("\n", " ")[:50])
        return genai_pb2.AnswerResponse(answer=resposta_final)

# =============================================================================
# Função Principal para Execução do Servidor
# =============================================================================
async def serve() -> None:
    server = aio.server()
    genai_pb2_grpc.add_GenAiServiceServicer_to_server(GenAiServiceServicer(), server)
    listen_addr = "[::]:50051"
    server.add_insecure_port(listen_addr)
    logger.info("Servidor escutando em %s", listen_addr)
    try:
        await server.start()
        logger.info("Servidor iniciado em %s", listen_addr)
        await server.wait_for_termination()
    except asyncio.CancelledError:
        logger.info("Serviço cancelado, desligando...")
    except Exception as e:
        logger.error("Erro inesperado: %s", str(e))
        raise
    finally:
        logger.info("Desligando servidor gRPC...")
        try:
            await asyncio.shield(server.stop(grace=5))
            logger.info("Servidor desligado com sucesso.")
        except asyncio.CancelledError:
            logger.warning("Shutdown interrompido.")

# =============================================================================
# Função Main que inicia o monitoramento de memória e o servidor
# =============================================================================
async def main():
    # Inicia a tarefa de monitoramento de memória em background
    memory_monitor_task = asyncio.create_task(monitor_memory())
    await serve()
    memory_monitor_task.cancel()
    try:
        await memory_monitor_task
    except asyncio.CancelledError:
        logger.info("Tarefa de monitoramento de memória cancelada.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupção manual (KeyboardInterrupt).")
