import os
import duckdb
import pyarrow.parquet as pq
from glob import glob

# Configurações
base_path = "./data"  # Substitua pelo caminho da sua pasta
folders = [
    "parquet_cnaes",
    "parquet_empresas",
    "parquet_estabelecimentos",
    "parquet_municipios",
    "parquet_naturezas",
    "parquet_paises",
    "parquet_qualificacoes",
    "parquet_socios",
]

# Conectar ao DuckDB (ou criar se não existir)
conn = duckdb.connect(database='my_dw.duckdb', read_only=False)

# Função para mapear tipos do PyArrow para tipos do DuckDB
def map_arrow_to_duckdb_type(arrow_type):
    if arrow_type == "large_string" or arrow_type == "string":
        return "VARCHAR"
    elif arrow_type == "int64":
        return "BIGINT"
    elif arrow_type == "int32":
        return "INTEGER"
    elif arrow_type == "float64":
        return "DOUBLE"
    elif arrow_type == "float32":
        return "FLOAT"
    elif arrow_type == "bool":
        return "BOOLEAN"
    elif arrow_type == "timestamp[us]":
        return "TIMESTAMP"
    elif arrow_type == "date32":
        return "DATE"
    else:
        raise ValueError(f"Tipo de dado não suportado: {arrow_type}")

# Função para processar Parquets em partes e evitar estouro de memória
def process_parquet_in_chunks(folder_path, table_name):
    parquet_files = glob(os.path.join(folder_path, "*.parquet"))
    if not parquet_files:
        print(f"Nenhum arquivo Parquet encontrado em {folder_path}")
        return
    
    # Criar a tabela no DuckDB (se não existir)
    first_file = parquet_files[0]
    first_batch = next(pq.ParquetFile(first_file).iter_batches())  # Lê o primeiro lote
    schema = first_batch.schema  # Obtém o schema do primeiro arquivo
    
    # Mapear os tipos do PyArrow para os tipos do DuckDB
    schema_mapped = [(field.name, map_arrow_to_duckdb_type(str(field.type))) for field in schema]
    
    # Criar a tabela no DuckDB
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(f'{name} {type}' for name, type in schema_mapped)})")
    
    # Processar cada arquivo Parquet em partes
    for file in parquet_files:
        print(f"Processando arquivo: {file}")
        parquet_file = pq.ParquetFile(file)
        
        # Ler e inserir lotes (batches) no DuckDB
        for batch in parquet_file.iter_batches():
            # Registrar o batch temporariamente no DuckDB
            conn.register('temp_batch', batch.to_pandas())  # Converte o batch para Pandas
            # Inserir os dados na tabela
            conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_batch")
    
    print(f"Tabela '{table_name}' populada com sucesso!")

# Processar cada pasta
for folder in folders:
    folder_path = os.path.join(base_path, folder)
    print(f"Processando pasta: {folder_path}")
    
    # Remover o prefixo "parquet_" do nome da tabela
    table_name = folder.replace("parquet_", "")
    
    # Processar os Parquets em partes
    process_parquet_in_chunks(folder_path, table_name)

# Fechar a conexão com o DuckDB
conn.close()
print("Processo concluído!")