import requests
from bs4 import BeautifulSoup
import os

def baixar_arquivos_cnpj(url_base, pasta_destino):
    """
    Função que acessa uma URL que lista vários arquivos .zip 
    e baixa todos esses arquivos para uma pasta destino local.
    """
    # Cria a pasta de destino se não existir
    if not os.path.exists(pasta_destino):
        os.makedirs(pasta_destino)

    # Faz a requisição à página
    try:
        response = requests.get(url_base)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar a URL: {e}")
        return

    # Usa BeautifulSoup para extrair links
    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.find_all('a')

    # Filtra somente arquivos que terminam em .zip
    for link in links:
        href = link.get('href')
        if href and href.lower().endswith('.zip'):
            # Monta a URL final de download
            download_url = href
            # Se o link for relativo, concatena com a URL base
            if not download_url.startswith('http'):
                download_url = url_base.rstrip('/') + '/' + download_url.lstrip('/')

            # Nome do arquivo (último pedaço da URL)
            nome_arquivo = download_url.split('/')[-1]
            caminho_arquivo = os.path.join(pasta_destino, nome_arquivo)

            # Verifica se o arquivo já existe
            if os.path.exists(caminho_arquivo):
                print(f"Arquivo já existe: {nome_arquivo}")
                continue

            print(f"Baixando: {nome_arquivo} ...")
            try:
                # Usar stream para baixar em chunks e evitar uso excessivo de memória
                with requests.get(download_url, stream=True) as r:
                    r.raise_for_status()
                    with open(caminho_arquivo, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                print(f"Download concluído: {nome_arquivo}")
            except requests.exceptions.RequestException as e:
                print(f"Erro ao baixar {nome_arquivo}: {e}")

if __name__ == "__main__":
    url = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2024-12/"
    pasta_destino = ".//data//arquivos_cnpj_2024_12"
    baixar_arquivos_cnpj(url, pasta_destino)
