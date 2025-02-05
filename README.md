# Chat Empresas

Este projeto Python é responsável por baixar, descompactar, converter e unificar arquivos de dados de empresas fornecidos pela Receita Federal do Brasil. Além disso, ele fornece uma interface de chat para interagir com os dados.

## Estrutura do Projeto

O projeto é dividido em duas partes principais:

1. **Download de Dados da Receita Federal** (`src/download_empresa`):
    - `src/download_empresa/download_dados_empresa.py`: Script para baixar arquivos .zip de uma URL.
    - `src/download_empresa/unzip_files.py`: Script para descompactar arquivos .zip.
    - `src/download_empresa/convert_files.py`: Script para converter arquivos de texto para o formato Parquet.

2. **Chat com Streamlit e DuckDB** (`src/chat`):
    - `src/chat/server.py`: Implementa o servidor DuckDB e funções relacionadas ao processamento de dados.
    - `src/chat/main.py`: Script principal que orquestra a execução da aplicação de chat.
    - `src/chat/base.py`: Contém funções auxiliares e definições de estado para o chat.
    - `src/chat/auth.py`: Gerencia a autenticação dos usuários.
    
## Requisitos

- Python 3.12+
- Pacote `uv` para gerenciar dependências

## Instalação de Dependências

Você pode instalar as dependências utilizando o pacote `uv`. Primeiro, instale o `uv`:

```bash
pip install uv
```

Em seguida, instale as dependências do projeto:

```bash
uv sync
source .venv/bin/activate
```

## Uso

### Baixar Arquivos

Para baixar os arquivos .zip de uma URL, execute o script [`src/download_empresa/download_dados_empresa.py`](src/download_empresa/download_dados_empresa.py):

```bash
.venv/bin/python ./src/download_empresa/download_dados_empresa.py
```

### Descompactar Arquivos

Para descompactar os arquivos .zip baixados, execute o script `unzip_files.py`:

```bash
.venv/bin/python ./src/download_empresa/unzip_files.py
```

### Converter Arquivos

Para converter os arquivos de texto descompactados para o formato Parquet e unificar os arquivos convertidos, execute o script `convert_files.py`:

```bash
.venv/bin/python ./src/download_empresa/convert_files.py
```

### Executar a Aplicação de Chat

Para executar a aplicação de chat, execute o script [`src/chat/server.py`](src/chat/server.py):

```bash
.venv/bin/python ./src/chat/server.py
```
Em outro terminal, execute o script [`src/chat/server.py`](src/chat/main.py):

```bash
streamlit run src/chat/main.py
```
## Estrutura dos Dados

### Empresas

- `CNPJ_BASICO`
- `RAZAO_SOCIAL`
- `NATUREZA_JURIDICA`
- `QUALIFICACAO_RESPONSAVEL`
- `CAPITAL_SOCIAL`
- `PORTE_EMPRESA`
- `ENTE_FEDERATIVO_RESPONSAVEL`

### Estabelecimentos

- `CNPJ_BASICO`
- `CNPJ_ORDEM`
- `CNPJ_DV`
- `IDENTIFICADOR_MATRIZ_FILIAL`
- `NOME_FANTASIA`
- `SITUACAO_CADASTRAL`
- `DATA_SITUACAO_CADASTRAL`
- `MOTIVO_SITUACAO_CADASTRAL`
- `NOME_CIDADE_EXTERIOR`
- `PAIS`
- `DATA_INICIO_ATIVIDADE`
- `CNAE_FISCAL_PRINCIPAL`
- `CNAE_FISCAL_SECUNDARIA`
- `TIPO_LOGRADOURO`
- `LOGRADOURO`
- `NUMERO`
- `COMPLEMENTO`
- `BAIRRO`
- `CEP`
- `UF`
- `MUNICIPIO`
- `DDD_1`
- `TELEFONE_1`
- `DDD_2`
- `TELEFONE_2`
- `DDD_FAX`
- `FAX`
- `CORREIO_ELETRONICO`
- `SITUACAO_ESPECIAL`
- `DATA_SITUACAO_ESPECIAL`

### Sócios

- `CNPJ_BASICO`
- `IDENTIFICADOR_SOCIO`
- `NOME_SOCIO`
- `CNPJ_CPF_SOCIO`
- `QUALIFICACAO_SOCIO`
- `DATA_ENTRADA_SOCIEDADE`
- `PAIS`
- `REPRESENTANTE_LEGAL`
- `NOME_REPRESENTANTE`
- `QUALIFICACAO_REPRESENTANTE`
- `FAIXA_ETARIA`

### Países

- `CODIGO_PAIS`
- `NOME_PAIS`

### Municípios

- `CODIGO_MUNICIPIO`
- `NOME_MUNICIPIO`

### Qualificações

- `CODIGO_QUALIFICACAO`
- `DESCRICAO_QUALIFICACAO`

### Naturezas Jurídicas

- `CODIGO_NATUREZA`
- `DESCRICAO_NATUREZA`

### CNAEs

- `CODIGO_CNAE`
- `DESCRICAO_CNAE`

## Licença

Este projeto está licenciado sob a Licença MIT. Veja o arquivo LICENSE para mais detalhes.
