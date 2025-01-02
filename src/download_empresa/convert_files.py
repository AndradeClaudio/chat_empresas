import pandas as pd
import os
from multiprocessing import Pool, cpu_count

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

    # Lê o arquivo TXT como um CSV delimitado por ponto e vírgula
    df = pd.read_csv(input_txt_path, sep=';', encoding='latin-1', header=None, names=column_names, on_bad_lines='skip', engine='python')

    # Converte o DataFrame para Parquet
    df.to_parquet(output_parquet_path, index=False)

def parse_estabele_to_parquet(input_txt_path, output_parquet_path):
    # Define o cabeçalho conforme o layout fornecido para estabelecimentos
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

    # Lê o arquivo TXT como um CSV delimitado por ponto e vírgula
    df = pd.read_csv(input_txt_path, sep=';', encoding='latin-1', header=None, names=column_names, on_bad_lines='skip', engine='python')

    # Converte o DataFrame para Parquet
    df.to_parquet(output_parquet_path, index=False)

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
    df = pd.read_csv(input_txt_path, sep=';', encoding='latin-1', header=None, names=column_names, on_bad_lines='skip', engine='python')
    df.to_parquet(output_parquet_path, index=False)

def parse_paises_to_parquet(input_txt_path, output_parquet_path):
    column_names = ["CODIGO_PAIS", "NOME_PAIS"]
    df = pd.read_csv(input_txt_path, sep=';', encoding='latin-1', header=None, names=column_names, on_bad_lines='skip', engine='python')
    df.to_parquet(output_parquet_path, index=False)

def parse_municipios_to_parquet(input_txt_path, output_parquet_path):
    column_names = ["CODIGO_MUNICIPIO", "NOME_MUNICIPIO"]
    df = pd.read_csv(input_txt_path, sep=';', encoding='latin-1', header=None, names=column_names, on_bad_lines='skip', engine='python')
    df.to_parquet(output_parquet_path, index=False)

def parse_qualificacoes_to_parquet(input_txt_path, output_parquet_path):
    column_names = ["CODIGO_QUALIFICACAO", "DESCRICAO_QUALIFICACAO"]
    df = pd.read_csv(input_txt_path, sep=';', encoding='latin-1', header=None, names=column_names, on_bad_lines='skip', engine='python')
    df.to_parquet(output_parquet_path, index=False)

def parse_naturezas_to_parquet(input_txt_path, output_parquet_path):
    column_names = ["CODIGO_NATUREZA", "DESCRICAO_NATUREZA"]
    df = pd.read_csv(input_txt_path, sep=';', encoding='latin-1', header=None, names=column_names, on_bad_lines='skip', engine='python')
    df.to_parquet(output_parquet_path, index=False)

def parse_cnaes_to_parquet(input_txt_path, output_parquet_path):
    column_names = ["CODIGO_CNAE", "DESCRICAO_CNAE"]
    df = pd.read_csv(input_txt_path, sep=';', encoding='latin-1', header=None, names=column_names, on_bad_lines='skip', engine='python')
    df.to_parquet(output_parquet_path, index=False)

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

    # Utiliza multiprocessing para processar até 4 arquivos ao mesmo tempo
    with Pool(processes=min(4, cpu_count())) as pool:
        pool.map(process_function, file_paths)

def unify_parquet_files(output_directory, unified_file_path):
    parquet_files = [os.path.join(output_directory, f) for f in os.listdir(output_directory) if f.endswith('.parquet')]
    df_list = [pd.read_parquet(file) for file in parquet_files]
    unified_df = pd.concat(df_list, ignore_index=True)
    unified_df.to_parquet(unified_file_path, index=False)

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
    unify_parquet_files(output_directory_empresas, "./data/unified_empresas.parquet")
    print("Unificação de arquivos de empresas concluída.")

    # Executa a conversão para todos os arquivos de estabelecimentos no diretório de entrada
    convert_all_files(input_directory, output_directory_estabelecimentos, ".ESTABELE", process_file)
    print("Conversão de arquivos de estabelecimentos concluída.")
    unify_parquet_files(output_directory_estabelecimentos, "./data/unified_estabelecimentos.parquet")
    print("Unificação de arquivos de estabelecimentos concluída.")

    # Executa a conversão para todos os arquivos de sócios no diretório de entrada
    convert_all_files(input_directory, output_directory_socios, ".SOCIOCS", process_file)
    print("Conversão de arquivos de sócios concluída.")
    unify_parquet_files(output_directory_socios, "./data/unified_socios.parquet")
    print("Unificação de arquivos de sócios concluída.")

    # Executa a conversão para todos os arquivos de países no diretório de entrada
    convert_all_files(input_directory, output_directory_paises, ".PAISCSV", process_file)
    print("Conversão de arquivos de países concluída.")
    unify_parquet_files(output_directory_paises, "./data/unified_paises.parquet")
    print("Unificação de arquivos de países concluída.")

    # Executa a conversão para todos os arquivos de municípios no diretório de entrada
    convert_all_files(input_directory, output_directory_municipios, ".MUNICSV", process_file)
    print("Conversão de arquivos de municípios concluída.")
    unify_parquet_files(output_directory_municipios, "./data/unified_municipios.parquet")
    print("Unificação de arquivos de municípios concluída.")

    # Executa a conversão para todos os arquivos de qualificações de sócios no diretório de entrada
    convert_all_files(input_directory, output_directory_qualificacoes, ".QUALSCSV", process_file)
    print("Conversão de arquivos de qualificações de sócios concluída.")
    unify_parquet_files(output_directory_qualificacoes, "./data/unified_qualificacoes.parquet")
    print("Unificação de arquivos de qualificações de sócios concluída.")

    # Executa a conversão para todos os arquivos de naturezas jurídicas no diretório de entrada
    convert_all_files(input_directory, output_directory_naturezas, ".NATJUCSV", process_file)
    print("Conversão de arquivos de naturezas jurídicas concluída.")
    unify_parquet_files(output_directory_naturezas, "./data/unified_naturezas.parquet")
    print("Unificação de arquivos de naturezas jurídicas concluída.")

    # Executa a conversão para todos os arquivos de CNAEs no diretório de entrada
    convert_all_files(input_directory, output_directory_cnaes, ".CNAECSV", process_file)
    print("Conversão de arquivos de CNAEs concluída.")
    unify_parquet_files(output_directory_cnaes, "./data/unified_cnaes.parquet")
    print("Unificação de arquivos de CNAEs concluída.")
