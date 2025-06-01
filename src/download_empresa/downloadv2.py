import os
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Configuração básica do logging
logging.basicConfig(
    level=logging.INFO,  # exibe mensagens de INFO, WARNING, ERROR e CRITICAL
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# URL base onde estão listadas as subpastas com os arquivos .zip
BASE_URL = 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/'

def get_links(url):
    """
    Retorna uma lista de todas as URLs (href) encontradas na página informada.
    """
    resp = requests.get(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')
    return [a['href'] for a in soup.find_all('a', href=True)]


def main():
    # Cria a pasta 'downloads' (caso não exista) onde serão salvos todos os subdiretórios
    os.makedirs('downloads', exist_ok=True)

    # Passo 1: pegar todas as “pastas” listadas na página base
    try:
        todas_as_entradas = get_links(BASE_URL)
    except Exception as e:
        logger.error(f"Falha ao acessar {BASE_URL}: {e}")
        return

    for entrada in todas_as_entradas:
        # Queremos apenas as entradas que terminam com '/' (subpastas),
        # e que não sejam "../" (Parent Directory)
        if not entrada.endswith('/') or entrada == '../':
            continue

        nome_subpasta = entrada.rstrip('/')
        subpasta_url = urljoin(BASE_URL, entrada)

        # Cria localmente uma pasta dentro de 'downloads' com o nome da subpasta
        pasta_local = os.path.join('downloads', nome_subpasta)
        os.makedirs(pasta_local, exist_ok=True)

        logger.info(f"Acessando subpasta: {nome_subpasta}")
        if nome_subpasta == '2025-05':
            try:
                arquivos_na_subpasta = get_links(subpasta_url)
            except Exception as e:
                logger.warning(f"  Não foi possível listar conteúdo de {subpasta_url}: {e}")
                continue
        else:
            logger.info(f"  [Ignorando] {nome_subpasta} — não é a subpasta de maio de 2025.")
            continue
        for arquivo in arquivos_na_subpasta:
            # Só queremos os arquivos que terminam em .zip
            if not arquivo.lower().endswith('.zip'):
                continue

            url_zip = urljoin(subpasta_url, arquivo)
            caminho_local_zip = os.path.join(pasta_local, arquivo)

            if os.path.exists(caminho_local_zip):
                logger.info(f"  [Já existe] {arquivo} — pulando download.")
                continue

            # Baixar em streaming
            logger.info(f"  Baixando {arquivo} ...")
            try:
                with requests.get(url_zip, stream=True) as r:
                    r.raise_for_status()
                    with open(caminho_local_zip, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                logger.info(f"  → {arquivo} baixado com sucesso.")
            except Exception as e:
                logger.error(f"  Erro ao baixar {arquivo}: {e}")
                # Remove arquivo incompleto, se criado
                if os.path.exists(caminho_local_zip):
                    try:
                        os.remove(caminho_local_zip)
                        logger.info(f"  Arquivo incompleto {arquivo} removido.")
                    except OSError as rm_err:
                        logger.warning(f"  Falha ao remover arquivo incompleto: {rm_err}")


if __name__ == '__main__':
    main()
