
        CREATE TABLE IF NOT EXISTS empresas (
          cnpj_basico VARCHAR, razao_social VARCHAR, natureza_juridica VARCHAR,
          qualificacao_responsavel VARCHAR, capital_social DOUBLE,
          porte VARCHAR, ente_federativo_responsavel VARCHAR
        );
        CREATE TABLE IF NOT EXISTS estabelecimentos (
          cnpj_basico VARCHAR, cnpj_ordem VARCHAR, cnpj_dv VARCHAR,
          identificador_matriz_filial VARCHAR, nome_fantasia VARCHAR,
          situacao_cadastral VARCHAR, data_situacao_cadastral DATE,
          motivo_situacao_cadastral VARCHAR, nome_cidade_exterior VARCHAR,
          pais VARCHAR, data_inicio_atividade DATE,
          cnae_fiscal_principal VARCHAR, cnae_fiscal_secundaria VARCHAR,
          tipo_logradouro VARCHAR, logradouro VARCHAR, numero VARCHAR,
          complemento VARCHAR, bairro VARCHAR, cep VARCHAR, uf VARCHAR,
          municipio VARCHAR, ddd1 VARCHAR, telefone1 VARCHAR,
          ddd2 VARCHAR, telefone2 VARCHAR, ddd_fax VARCHAR, fax VARCHAR,
          email VARCHAR, situacao_especial VARCHAR, data_situacao_especial DATE
        );
        CREATE TABLE IF NOT EXISTS simples (
          cnpj_basico VARCHAR, opcao_simples VARCHAR,
          data_opcao_simples DATE, data_exclusao_simples DATE,
          opcao_mei VARCHAR, data_opcao_mei DATE, data_exclusao_mei DATE
        );
        CREATE TABLE IF NOT EXISTS socios (
          cnpj_basico VARCHAR, identificador_socio VARCHAR,
          nome_socio_razao_social VARCHAR, cpf_cnpj_socio VARCHAR,
          qualificacao_socio VARCHAR, data_entrada_sociedade DATE,
          pais VARCHAR, cpf_representante_legal VARCHAR,
          nome_representante VARCHAR, qualificacao_representante VARCHAR,
          faixa_etaria VARCHAR
        );
        CREATE TABLE IF NOT EXISTS cnaes         (codigo VARCHAR, descricao VARCHAR);
        CREATE TABLE IF NOT EXISTS naturezas     (codigo VARCHAR, descricao VARCHAR);
        CREATE TABLE IF NOT EXISTS municipios    (codigo VARCHAR, descricao VARCHAR);
        CREATE TABLE IF NOT EXISTS paises        (codigo VARCHAR, descricao VARCHAR);
        CREATE TABLE IF NOT EXISTS qualificacoes (codigo VARCHAR, descricao VARCHAR);
        CREATE TABLE IF NOT EXISTS motivos       (codigo VARCHAR, descricao VARCHAR);
        