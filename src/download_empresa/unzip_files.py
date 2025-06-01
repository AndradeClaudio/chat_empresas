import os
import zipfile
import multiprocessing

def unzip_file(params):
    """
    Função para descompactar o arquivo.
    Recebe uma tupla (file_path, destination_directory).
    """
    file_path, destination_directory = params
    print(f"Iniciando descompactação: {file_path}")
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(destination_directory)
    print(f"Finalizado: {file_path}")
    return file_path  # Retornamos para fins de log ou debug, se quiser

def unzip_files(source_directory, destination_directory):
    # Lista apenas arquivos .zip
    zip_files = [
        os.path.join(source_directory, f) 
        for f in os.listdir(source_directory) 
        if f.endswith(".zip")
    ]

    # Cria uma lista de parâmetros para enviar ao Pool
    # Cada elemento será (caminho_completo_do_arquivo_zip, diretorio_destino)
    params_list = [(zip_file, destination_directory) for zip_file in zip_files]

    # Define a quantidade de processos que deseja usar
    # Se deixar None, o Pool usa o número de CPUs disponíveis
    # Exemplo fixo: processes=4
    with multiprocessing.Pool() as pool:
        # Executa unzip_file em paralelo para cada item da lista
        results = pool.map(unzip_file, params_list)

    # Aqui, todos os processos já terminaram
    print("Todos os processos terminaram.")
    # results contém o retorno de cada chamada de unzip_file
    # se precisar, pode usar essa lista para logs ou tratamentos adicionais

if __name__ == "__main__":
    source_directory = "./downloads/2025-05"
    destination_directory = "./data/unzipped_files_2025_05"
    unzip_files(source_directory, destination_directory)
