import os
import fireducks.pandas as pd
from tqdm import tqdm

def log_and_parse(
    input_txt_path,
    output_parquet_path_prefix,
    column_names,
    chunksize=500_000
):
    """
    Lê o arquivo em chunks (lotes) para evitar estouro de memória e travamento.
    Gera um arquivo parquet por chunk, salvando diretamente em disco.
    
    Parâmetros:
    -----------
    - input_txt_path: str
        Caminho do arquivo de entrada (CSV ou TXT).
    - output_parquet_path_prefix: str
        Prefixo do caminho/arquivo de saída dos parquet. 
        Ex.: "dados_parquet" irá gerar "dados_parquet_chunk_0.parquet", 
        "dados_parquet_chunk_1.parquet", etc.
    - column_names: list
        Lista com os nomes das colunas (se o CSV não tiver header).
    - chunksize: int
        Tamanho de cada chunk (número de linhas). 
        Ajuste conforme sua disponibilidade de memória.
    """

    # Conta total de linhas para ter progresso no tqdm.
    # Se seu arquivo for muito grande, talvez prefira outro método de contagem.
    total_lines = sum(1 for _ in open(input_txt_path, encoding='latin-1'))

    # Cria iterador de chunks
    chunk_iterator = pd.read_csv(
        input_txt_path,
        sep=';',            # Ajuste conforme seu delimitador
        names=column_names, # Caso não haja header no arquivo
        encoding='latin-1', 
        chunksize=chunksize,
        dtype=str,          # Força tudo como string (opcional)
        on_bad_lines='skip' # Se houver linhas problemáticas, pula
    )

    pbar = tqdm(total=total_lines, desc=f"Lendo {os.path.basename(input_txt_path)}", unit="linhas")
    
    chunk_count = 0
    
    for chunk_df in chunk_iterator:
        # Define o nome do arquivo parquet para este chunk
        # Exemplo: se output_parquet_path_prefix="saida_parquet", gera
        # "saida_parquet_chunk_0.parquet", "saida_parquet_chunk_1.parquet", ...
        output_file = f"{output_parquet_path_prefix}_chunk_{chunk_count}.parquet"
        
        # Salva o chunk em parquet
        chunk_df.to_parquet(output_file, index=False)
        
        # Atualiza o contador de chunk
        chunk_count += 1
        
        # Atualiza a barra de progresso
        pbar.update(len(chunk_df))
        
        # Libera o DataFrame explicitamente (opcional, mas pode ajudar)
        del chunk_df
    
    pbar.close()
    
    print(f"Processo concluído! Foram gerados {chunk_count} arquivos Parquet.")

def parse_txt_to_parquet(input_txt_path, output_parquet_path):
    column_names = [
        "CNPJ_BASICO",
        "RAZAO_SOCIAL",
        "NATUREZA_JURIDICA",
        "QUALIFICACAO_RESPONSAVEL",
        "CAPITAL_SOCIAL",
        "PORTE_EMPRESA",
        "ENTE_FEDERATIVO_RESPONSAVEL"
    ]
    log_and_parse(input_txt_path, output_parquet_path, column_names)

def parse_estabele_to_parquet(input_txt_path, output_parquet_path):
    column_names = [
        "CNPJ_BASICO",
        "CNPJ_ORDEM",
        "CNPJ_DV",
        "IDENTIFICADOR_MATRIZ_FILIAL",
        "NOME_FANTASIA",
        "SITUACAO_CADASTRAL",
        "DATA_SITUACAO_CADASTRAL",
        "MOTIVO_SITUACAO_CADASTRAL",
        "NOME_CIDADE_EXTERIOR",
        "PAIS",
        "DATA_INICIO_ATIVIDADE",
        "CNAE_FISCAL_PRINCIPAL",
        "CNAE_FISCAL_SECUNDARIA",
        "TIPO_LOGRADOURO",
        "LOGRADOURO",
        "NUMERO",
        "COMPLEMENTO",
        "BAIRRO",
        "CEP",
        "UF",
        "MUNICIPIO",
        "DDD_1",
        "TELEFONE_1",
        "DDD_2",
        "TELEFONE_2",
        "DDD_FAX",
        "FAX",
        "CORREIO_ELETRONICO",
        "SITUACAO_ESPECIAL",
        "DATA_SITUACAO_ESPECIAL"
    ]
    log_and_parse(input_txt_path, output_parquet_path, column_names)

def parse_socios_to_parquet(input_txt_path, output_parquet_path):
    column_names = [
        "CNPJ_BASICO",
        "IDENTIFICADOR_SOCIO",
        "NOME_SOCIO",
        "CNPJ_CPF_SOCIO",
        "QUALIFICACAO_SOCIO",
        "DATA_ENTRADA_SOCIEDADE",
        "PAIS",
        "REPRESENTANTE_LEGAL",
        "NOME_REPRESENTANTE",
        "QUALIFICACAO_REPRESENTANTE",
        "FAIXA_ETARIA"
    ]
    log_and_parse(input_txt_path, output_parquet_path, column_names)

def parse_paises_to_parquet(input_txt_path, output_parquet_path):
    column_names = ["CODIGO_PAIS", "NOME_PAIS"]
    log_and_parse(input_txt_path, output_parquet_path, column_names)

def parse_municipios_to_parquet(input_txt_path, output_parquet_path):
    column_names = ["CODIGO_MUNICIPIO", "NOME_MUNICIPIO"]
    log_and_parse(input_txt_path, output_parquet_path, column_names)

def parse_qualificacoes_to_parquet(input_txt_path, output_parquet_path):
    column_names = ["CODIGO_QUALIFICACAO", "DESCRICAO_QUALIFICACAO"]
    log_and_parse(input_txt_path, output_parquet_path, column_names)

def parse_naturezas_to_parquet(input_txt_path, output_parquet_path):
    column_names = ["CODIGO_NATUREZA", "DESCRICAO_NATUREZA"]
    log_and_parse(input_txt_path, output_parquet_path, column_names)

def parse_cnaes_to_parquet(input_txt_path, output_parquet_path):
    column_names = ["CODIGO_CNAE", "DESCRICAO_CNAE"]
    log_and_parse(input_txt_path, output_parquet_path, column_names)

def process_file(file_paths):
    input_file_path, output_file_path = file_paths
    print(f"Convertendo: {input_file_path} -> {output_file_path}")
    if input_file_path.endswith(".EMPRECSV"):
        parse_txt_to_parquet(input_file_path, output_file_path)
    elif input_file_path.endswith(".ESTABELE"):
        parse_estabele_to_parquet(input_file_path, output_file_path)
    elif input_file_path.endswith(".SOCIOCSV"):
        parse_socios_to_parquet(input_file_path, output_file_path)
    elif input_file_path.endswith(".PAISCSV"):
        parse_paises_to_parquet(input_file_path, output_file_path)
    elif input_file_path.endswith(".MUNICCSV"):
        parse_municipios_to_parquet(input_file_path, output_file_path)
    elif input_file_path.endswith(".QUALSCSV"):
        parse_qualificacoes_to_parquet(input_file_path, output_file_path)
    elif input_file_path.endswith(".NATJUCSV"):
        parse_naturezas_to_parquet(input_file_path, output_file_path)
    elif input_file_path.endswith(".CNAECSV"):
        parse_cnaes_to_parquet(input_file_path, output_file_path)

def convert_all_files(input_directory, output_directory, file_extension, process_function):
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    file_paths = []
    for filename in os.listdir(input_directory):
        if filename.endswith(file_extension):
            input_file_path = os.path.join(input_directory, filename)
            output_file_name = f"{os.path.splitext(filename)[0]}.parquet"
            output_file_path = os.path.join(output_directory, output_file_name)
            file_paths.append((input_file_path, output_file_path))

    # Agora processamos cada arquivo diretamente, sem usar Pool
    for file_path in tqdm(file_paths, total=len(file_paths), desc="Convertendo arquivos"):
        process_function(file_path)

if __name__ == "__main__":
    input_directory = "./data/unzipped_files_2025_01"
    output_directory_empresas = "./data/parquet_empresas"
    output_directory_estabelecimentos = "./data/parquet_estabelecimentos"
    output_directory_socios = "./data/parquet_socios"
    output_directory_paises = "./data/parquet_paises"
    output_directory_municipios = "./data/parquet_municipios"
    output_directory_qualificacoes = "./data/parquet_qualificacoes"
    output_directory_naturezas = "./data/parquet_naturezas"
    output_directory_cnaes = "./data/parquet_cnaes"

    convert_all_files(input_directory, output_directory_empresas, ".EMPRECSV", process_file)
    print("Conversão de arquivos de empresas concluída.")

    convert_all_files(input_directory, output_directory_estabelecimentos, ".ESTABELE", process_file)
    print("Conversão de arquivos de estabelecimentos concluída.")

    convert_all_files(input_directory, output_directory_socios, ".SOCIOCSV", process_file)
    print("Conversão de arquivos de sócios concluída.")

    convert_all_files(input_directory, output_directory_paises, ".PAISCSV", process_file)
    print("Conversão de arquivos de países concluída.")

    convert_all_files(input_directory, output_directory_municipios, ".MUNICCSV", process_file)
    print("Conversão de arquivos de municípios concluída.")

    convert_all_files(input_directory, output_directory_qualificacoes, ".QUALSCSV", process_file)
    print("Conversão de arquivos de qualificações de sócios concluída.")

    convert_all_files(input_directory, output_directory_naturezas, ".NATJUCSV", process_file)
    print("Conversão de arquivos de naturezas jurídicas concluída.")

    convert_all_files(input_directory, output_directory_cnaes, ".CNAECSV", process_file)
    print("Conversão de arquivos de CNAEs concluída.")
