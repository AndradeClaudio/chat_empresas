import duckdb
import gc

# Se desejar, pode manter o arquivo do banco para persistir os resultados;
# Caso contrário, ao não especificar um arquivo, o banco será criado em memória.
DB_FILE = 'dados_empresas.duckdb'  # Opcional, se preferir persistência em disco

# Abre a conexão (em memória ou com arquivo, conforme sua preferência)
conn = duckdb.connect(DB_FILE)  # ou apenas duckdb.connect() para uma conexão em memória
conn.execute("PRAGMA memory_limit='8GB';")

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

# Recupera e exibe os resultados da consulta
print("Recuperando resultados da consulta...")
result = conn.execute("""SELECT CNPJ_BASICO, RAZAO_SOCIAL, NATUREZA_JURIDICA, CAPITAL_SOCIAL, PORTE_EMPRESA, NOME_FANTASIA, CNAE_FISCAL_PRINCIPAL, NOME_SOCIO, CNPJ_CPF_SOCIO, QUALIFICACAO_SOCIO
FROM dados_empresas.main.resultados_consulta
WHERE UPPER(RAZAO_SOCIAL) LIKE UPPER('%VACCINAR%');""").fetchdf()
print(result)

# Fecha a conexão
conn.close()
