import os
import polars as pl
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

def parse_txt_to_parquet(input_txt_path, output_parquet_path):
    # Define o cabeçalho conforme o layout fornecido
    column_names = [
        "CNPJ_BASICO",
        "RAZAO_SOCIAL",
        "NATUREZA_JURIDICA",
        "QUALIFICACAO_RESPONSAVEL",
        "CAPITAL_SOCIAL",
        "PORTE_EMPRESA",
        "ENTE_FEDERATIVO_RESPONSAVEL"
    ]

    # Lê o arquivo CSV usando Polars
    df = pl.read_csv(
        input_txt_path,
        separator=';',
        has_header=False,
        new_columns=column_names,
        encoding='latin-1',
        # Removemos ignore_errors=True
        # e, caso sua versão do Polars suporte, podemos usar `on_error="raise"`:
        #on_error="raise"
    )

    # Converte o DataFrame para Parquet
    df.write_parquet(output_parquet_path)

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

    df = pl.read_csv(
        input_txt_path,
        separator=';',
        has_header=False,
        new_columns=column_names,
        encoding='latin-1',
        quote_char='"',
        # Removemos ignore_errors=True
        #on_error="raise",
        infer_schema_length=10000
    )
    df.write_parquet(output_parquet_path)

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

    df = pl.read_csv(
        input_txt_path,
        separator=';',
        has_header=False,
        new_columns=column_names,
        encoding='latin-1',
        # Removemos ignore_errors=True
        #on_error="raise"
    )
    df.write_parquet(output_parquet_path)

def parse_paises_to_parquet(input_txt_path, output_parquet_path):
    column_names = ["CODIGO_PAIS", "NOME_PAIS"]
    df = pl.read_csv(
        input_txt_path,
        separator=';',
        has_header=False,
        new_columns=column_names,
        encoding='latin-1',
        # Removemos ignore_errors=True
        #on_error="raise"
    )
    df.write_parquet(output_parquet_path)

def parse_municipios_to_parquet(input_txt_path, output_parquet_path):
    column_names = ["CODIGO_MUNICIPIO", "NOME_MUNICIPIO"]
    df = pl.read_csv(
        input_txt_path,
        separator=';',
        has_header=False,
        new_columns=column_names,
        encoding='latin-1',
        # Removemos ignore_errors=True
        #on_error="raise"
    )
    df.write_parquet(output_parquet_path)

def parse_qualificacoes_to_parquet(input_txt_path, output_parquet_path):
    column_names = ["CODIGO_QUALIFICACAO", "DESCRICAO_QUALIFICACAO"]
    df = pl.read_csv(
        input_txt_path,
        separator=';',
        has_header=False,
        new_columns=column_names,
        encoding='latin-1',
        # Removemos ignore_errors=True
        #on_error="raise"
    )
    df.write_parquet(output_parquet_path)

def parse_naturezas_to_parquet(input_txt_path, output_parquet_path):
    column_names = ["CODIGO_NATUREZA", "DESCRICAO_NATUREZA"]
    df = pl.read_csv(
        input_txt_path,
        separator=';',
        has_header=False,
        new_columns=column_names,
        encoding='latin-1',
        # Removemos ignore_errors=True
        #on_error="raise"
    )
    df.write_parquet(output_parquet_path)

def parse_cnaes_to_parquet(input_txt_path, output_parquet_path):
    column_names = ["CODIGO_CNAE", "DESCRICAO_CNAE"]
    df = pl.read_csv(
        input_txt_path,
        separator=';',
        has_header=False,
        new_columns=column_names,
        encoding='latin-1',
        # Removemos ignore_errors=True
        #on_error="raise"
    )
    df.write_parquet(output_parquet_path)

def process_file(file_paths):
    input_file_path, output_file_path = file_paths
    print(f"Convertendo: {input_file_path} -> {output_file_path}")
    if input_file_path.endswith(".EMPRECSV"):
        parse_txt_to_parquet(input_file_path, output_file_path)
    elif input_file_path.endswith(".ESTABELE"):
        parse_estabele_to_parquet(input_file_path, output_file_path)
    elif input_file_path.endswith(".SOCIOCS"):
        parse_socios_to_parquet(input_file_path, output_file_path)
    elif input_file_path.endswith(".PAISCSV"):
        parse_paises_to_parquet(input_file_path, output_file_path)
    elif input_file_path.endswith(".MUNICSV"):
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

    # Utiliza multiprocessing para processar até 2 arquivos ao mesmo tempo
    with Pool(processes=min(1, cpu_count())) as pool:
        # Usando 'imap' em vez de 'map' para permitir a iteração e exibir a barra de progresso
        for _ in tqdm(pool.imap(process_function, file_paths), total=len(file_paths), desc="Convertendo arquivos"):
            pass

if __name__ == "__main__":
    # Caminhos de exemplo para os diretórios de entrada e saída
    input_directory = "./data/unzipped_files"
    output_directory_empresas = "./data/parquet_empresas"
    output_directory_estabelecimentos = "./data/parquet_estabelecimentos"
    output_directory_socios = "./data/parquet_socios"
    output_directory_paises = "./data/parquet_paises"
    output_directory_municipios = "./data/parquet_municipios"
    output_directory_qualificacoes = "./data/parquet_qualificacoes"
    output_directory_naturezas = "./data/parquet_naturezas"
    output_directory_cnaes = "./data/parquet_cnaes"

    # Executa a conversão para todos os arquivos de empresas no diretório de entrada
    convert_all_files(input_directory, output_directory_empresas, ".EMPRECSV", process_file)
    print("Conversão de arquivos de empresas concluída.")

    # Executa a conversão para todos os arquivos de estabelecimentos no diretório de entrada
    convert_all_files(input_directory, output_directory_estabelecimentos, ".ESTABELE", process_file)
    print("Conversão de arquivos de estabelecimentos concluída.")

    # Executa a conversão para todos os arquivos de sócios no diretório de entrada
    convert_all_files(input_directory, output_directory_socios, ".SOCIOCS", process_file)
    print("Conversão de arquivos de sócios concluída.")

    # Executa a conversão para todos os arquivos de países no diretório de entrada
    convert_all_files(input_directory, output_directory_paises, ".PAISCSV", process_file)
    print("Conversão de arquivos de países concluída.")

    # Executa a conversão para todos os arquivos de municípios no diretório de entrada
    convert_all_files(input_directory, output_directory_municipios, ".MUNICSV", process_file)
    print("Conversão de arquivos de municípios concluída.")

    # Executa a conversão para todos os arquivos de qualificações de sócios no diretório de entrada
    convert_all_files(input_directory, output_directory_qualificacoes, ".QUALSCSV", process_file)
    print("Conversão de arquivos de qualificações de sócios concluída.")

    # Executa a conversão para todos os arquivos de naturezas jurídicas no diretório de entrada
    convert_all_files(input_directory, output_directory_naturezas, ".NATJUCSV", process_file)
    print("Conversão de arquivos de naturezas jurídicas concluída.")

    # Executa a conversão para todos os arquivos de CNAEs no diretório de entrada
    convert_all_files(input_directory, output_directory_cnaes, ".CNAECSV", process_file)
    print("Conversão de arquivos de CNAEs concluída.")
