import duckdb

# Diretórios e colunas das tabelas
diretorio_empresas = 'data/parquet_empresas/*.parquet'
diretorio_estabelecimentos = 'data/parquet_estabelecimentos/*.parquet'
diretorio_socios = 'data/parquet_socios/*.parquet'

# Consulta SQL para unir as tabelas
query = f"""
WITH empresas AS (
    SELECT 
        CNPJ_BASICO, 
        RAZAO_SOCIAL, 
        NATUREZA_JURIDICA, 
        QUALIFICACAO_RESPONSAVEL, 
        CAPITAL_SOCIAL, 
        PORTE_EMPRESA, 
        ENTE_FEDERATIVO_RESPONSAVEL
    FROM read_parquet('{diretorio_empresas}')
),
estabelecimentos AS (
    SELECT 
        CNPJ_BASICO, 
        CNPJ_ORDEM, 
        CNPJ_DV, 
        IDENTIFICADOR_MATRIZ_FILIAL, 
        NOME_FANTASIA, 
        SITUACAO_CADASTRAL, 
        DATA_SITUACAO_CADASTRAL, 
        MOTIVO_SITUACAO_CADASTRAL, 
        NOME_CIDADE_EXTERIOR, 
        PAIS, 
        DATA_INICIO_ATIVIDADE, 
        CNAE_FISCAL_PRINCIPAL, 
        CNAE_FISCAL_SECUNDARIA, 
        TIPO_LOGRADOURO, 
        LOGRADOURO, 
        NUMERO, 
        COMPLEMENTO, 
        BAIRRO, 
        CEP, 
        UF, 
        MUNICIPIO, 
        DDD_1, 
        TELEFONE_1, 
        DDD_2, 
        TELEFONE_2, 
        DDD_FAX, 
        FAX, 
        CORREIO_ELETRONICO, 
        SITUACAO_ESPECIAL, 
        DATA_SITUACAO_ESPECIAL
    FROM read_parquet('{diretorio_estabelecimentos}')
),
socios AS (
    SELECT 
        CNPJ_BASICO, 
        IDENTIFICADOR_SOCIO, 
        NOME_SOCIO, 
        CNPJ_CPF_SOCIO, 
        QUALIFICACAO_SOCIO, 
        DATA_ENTRADA_SOCIEDADE, 
        PAIS, 
        REPRESENTANTE_LEGAL, 
        NOME_REPRESENTANTE, 
        QUALIFICACAO_REPRESENTANTE, 
        FAIXA_ETARIA
    FROM read_parquet('{diretorio_socios}')
)
SELECT 
   *
FROM socios AS e
WHERE e.NOME_SOCIO='NELSON DE SOUZA LOPES'
LIMIT 100
"""
# Executar a consulta no DuckDB
result = duckdb.query(query).to_df()
# Exibir os resultados
print(result)
