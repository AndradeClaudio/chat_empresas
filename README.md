# Projeto de Download de Arquivos CNPJ

Este projeto é uma ferramenta para baixar arquivos .zip contendo dados de CNPJ a partir de uma URL específica e salvá-los em um diretório local. Ele utiliza as bibliotecas `requests` para fazer requisições HTTP e `BeautifulSoup` para analisar o conteúdo HTML da página.

## Estrutura do Projeto

- `src/download_empresa/download_dados_empresa.py`: Script principal que contém a função `baixar_arquivos_cnpj` para baixar os arquivos .zip.
- `src/download_empresa/unzip_files.py`: Script para descompactar os arquivos .zip baixados.
- `src/main.py`: Script de chat do streamlit


## Como Usar

1. Certifique-se de ter as bibliotecas necessárias instaladas:
    ```sh
    pip install -r requirements.txt
    ```

2. Execute o script principal para baixar os arquivos:
    ```sh
    python src/download_empresa/download_dados_empresa.py
    ```

3. Execute o script para descompactar os arquivos baixados:
    ```sh
    python src/download_empresa/unzip_files.py
    ```

## Estrutura de Diretórios

- `data`: Diretório onde os arquivos baixados serão salvos.
- `src`: Diretório contendo os scripts principais do projeto.


