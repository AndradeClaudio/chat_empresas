import os

# Função para converter o arquivo de latin-1 para utf-8
def converter_arquivo(arquivo_origem, arquivo_destino):
    with open(arquivo_origem, 'r', encoding='latin-1') as f_in:
        with open(arquivo_destino, 'w', encoding='utf-8') as f_out:
            for linha in f_in:
                f_out.write(linha)

# Função para percorrer todos os arquivos na pasta e convertê-los
def converter_pasta(caminho_pasta_origem, caminho_pasta_destino):
    for raiz, _, arquivos in os.walk(caminho_pasta_origem):
        for arquivo in arquivos:
            caminho_arquivo_origem = os.path.join(raiz, arquivo)
            
            # Definindo o caminho do arquivo de destino, mantendo a estrutura de diretórios
            caminho_relativo = os.path.relpath(caminho_arquivo_origem, caminho_pasta_origem)
            caminho_destino = os.path.join(caminho_pasta_destino, caminho_relativo)
            
            # Garantir que o diretório de destino exista
            os.makedirs(os.path.dirname(caminho_destino), exist_ok=True)
            
            try:
                # Convertendo o arquivo
                converter_arquivo(caminho_arquivo_origem, caminho_destino)
                print(f'Arquivo convertido: {caminho_arquivo_origem} -> {caminho_destino}')
                
            except Exception as e:
                print(f'Erro ao converter o arquivo {caminho_arquivo_origem}: {e}')

# Caminhos das pastas origem e destino
caminho_da_pasta_origem = 'data/unzipped_files_2025_01'
caminho_da_pasta_destino = 'data/arquivos_convertidos'

# Executar a conversão
converter_pasta(caminho_da_pasta_origem, caminho_da_pasta_destino)
