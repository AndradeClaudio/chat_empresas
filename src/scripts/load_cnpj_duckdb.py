#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
load_cnpj_duckdb.py – versão robusta p/ DuckDB 1.2+

• Importa todos os CSVs da Receita Federal (qualquer lote/ano) direto
  para DuckDB, detectando e contornando:
    - arquivos ZIP esquecidos na pasta,
    - lotes que vieram em UTF-8,
    - linhas mal-formadas (aspas duplicadas etc.).
• Faz fallback automático: tenta Latin-1 e depois UTF-8.
• capital_social entra como texto e é convertido para DOUBLE
  após a carga.
• Exporta as tabelas para Parquet (compressão Zstd).

Instalação:
    pip install duckdb>=1.2 tqdm
"""

from __future__ import annotations

import os
from pathlib import Path
from codecs import BOM_UTF8

import duckdb
from duckdb import InvalidInputException
from tqdm import tqdm

# ───────── CONFIGURAÇÕES ─────────
RAW_DIR     = Path(r"C:\Users\andrade\projetos\chat_empresas\data\unzipped_files_2025_05")
DB_PATH     = Path(r"C:\Users\andrade\projetos\chat_empresas\cnpj.duckdb")
PARQUET_DIR = RAW_DIR.parent / "parquet"
THREADS     = min(os.cpu_count() or 1, 4)          # ajuste se quiser
PARQUET_DIR.mkdir(parents=True, exist_ok=True)
# ─────────────────────────────────


def connect_db() -> duckdb.DuckDBPyConnection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"PRAGMA threads={THREADS};")
    return con


def create_schema(con: duckdb.DuckDBPyConnection) -> None:
    ddl = r"""
    CREATE TABLE IF NOT EXISTS empresas (
      cnpj_basico VARCHAR,
      razao_social VARCHAR,
      natureza_juridica VARCHAR,
      qualificacao_responsavel VARCHAR,
      capital_social VARCHAR,                -- texto → DOUBLE depois
      porte VARCHAR,
      ente_federativo_responsavel VARCHAR
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
    """
    for stmt in ddl.split(";"):
        if stmt.strip():
            con.execute(stmt + ";")


def load_files(con: duckdb.DuckDBPyConnection) -> None:
    """Importa tabelas arquivo-por-arquivo com fallback de encoding."""
    mapping = [
        ("*EMPRECSV", "empresas",         False),
        ("*ESTABELE", "estabelecimentos", True),
        ("*SOCIOCSV", "socios",           True),
        ("*SIMPLES*", "simples",          True),
        ("*CNAECSV",  "cnaes",            False),
        ("*NATJUCSV", "naturezas",        False),
        ("*MUNICCSV", "municipios",       False),
        ("*PAISCSV",  "paises",           False),
        ("*QUALSCSV", "qualificacoes",    False),
        ("*MOTICCSV", "motivos",          False),
    ]

    for pattern, table, has_null in tqdm(mapping, desc="Importando"):
        for csv in sorted(RAW_DIR.glob(pattern)):
            # 1) pula arquivos óbvios não-CSV (ZIP etc.)
            with open(csv, "rb") as fh:
                head = fh.read(4)
            if head.startswith(b"PK\x03\x04"):            # ZIP signature
                print(f"⚠️  {csv.name} é ZIP → ignorado")
                continue

            # 2) define ordem de tentativa de encoding
            if head.startswith(BOM_UTF8):
                encodings = ("utf-8",)                    # BOM garante UTF-8
            else:
                encodings = ("latin-1", "utf-8")

            for enc in encodings:
                opts = [
                    "DELIMITER ';'",
                    "QUOTE '\"'",
                    "ESCAPE '\"'",            # lida com "" dentro do campo
                    "HEADER FALSE",
                    "DATEFORMAT '%Y%m%d'",
                    f"ENCODING '{enc}'",
                    "AUTO_DETECT FALSE",
                    "IGNORE_ERRORS TRUE",     # pula linhas realmente quebradas
                ]
                if has_null:
                    opts.append("NULL '00000000'")

                try:
                    con.execute(
                        f"COPY {table} FROM '{csv.as_posix()}' ({', '.join(opts)});"
                    )
                    break                      # sucesso → sai do loop encodings
                except InvalidInputException as e:
                    # Se o erro é “File is not … encoded”, tenta próximo encoding
                    msg = str(e).lower()
                    if ("not" in msg) and ("encoded" in msg):
                        continue
                    raise                     # outro tipo de erro → propaga
            else:
                print(f"⚠️  {csv.name}: não pôde ser importado (encoding).")

    # ---- capital_social: VARCHAR → DOUBLE ----
    con.execute("""
        ALTER TABLE empresas
        ALTER COLUMN capital_social
        TYPE DOUBLE
        USING CASE
                 WHEN capital_social IS NULL OR capital_social = ''
                 THEN NULL
                 ELSE
                   REPLACE(
                     REPLACE(capital_social, '.', ''), ',', '.')::DOUBLE
             END;
    """)


def export_parquets(con: duckdb.DuckDBPyConnection) -> None:
    tables = [
        "empresas", "estabelecimentos", "socios", "simples",
        "cnaes", "naturezas", "municipios", "paises",
        "qualificacoes", "motivos",
    ]
    for tbl in tables:
        out_file = PARQUET_DIR / f"{tbl}.parquet"
        con.execute(
            f"COPY (SELECT * FROM {tbl}) "
            f"TO '{out_file.as_posix()}' "
            "(FORMAT 'parquet', COMPRESSION 'zstd');"
        )


def main() -> None:
    con = connect_db()
    create_schema(con)
    load_files(con)
    export_parquets(con)
    print(f"\n✅ Banco pronto em {DB_PATH}")
    print(f"   Parquets salvos em {PARQUET_DIR}")


if __name__ == "__main__":
    main()
