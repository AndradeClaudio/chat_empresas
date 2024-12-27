# Projeto de Download de Arquivos CNPJ

Este projeto é uma ferramenta para baixar arquivos .zip contendo dados de CNPJ a partir de uma URL específica e salvá-los em um diretório local. Ele utiliza as bibliotecas `requests` para fazer requisições HTTP e `BeautifulSoup` para analisar o conteúdo HTML da página.

## Estrutura do Projeto

- `src/download_dados_empresa.py`: Script principal que contém a função `baixar_arquivos_cnpj` para baixar os arquivos .zip.
- `src/main.py`: Script de entrada que pode ser usado para iniciar o processo de download.
- `src/unzip_files.py`: Script para descompactar os arquivos .zip baixados.


## Como Usar

1. Certifique-se de ter as bibliotecas necessárias instaladas:
    ```sh
    pip install -r requirements.txt
    ```

2. Execute o script principal para baixar os arquivos:
    ```sh
    python src/download_dados_empresa.py
    ```

3. (Opcional) Execute o script para descompactar os arquivos baixados:
    ```sh
    python src/unzip_files.py
    ```

4. (Opcional) Atualize o [README.md](http://_vscodecontentref_/1) com a lista de arquivos na pasta atual:
    ```sh
    python src/update_readme.py
    ```

## Estrutura de Diretórios

- [data](http://_vscodecontentref_/2): Diretório onde os arquivos baixados serão salvos.
- [qdrant_storage](http://_vscodecontentref_/3): Diretório de armazenamento do Qdrant.
- [src](http://_vscodecontentref_/4): Diretório contendo os scripts principais do projeto.

## Exemplo de Uso

No script [download_dados_empresa.py](http://_vscodecontentref_/5), a URL base e a pasta de destino são definidas da seguinte forma:
```python
if __name__ == "__main__":
    url = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2024-12/"
    pasta_destino = ".//data//arquivos_cnpj_2024_12"
    baixar_arquivos_cnpj(url, pasta_destino)
