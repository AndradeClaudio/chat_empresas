#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
load_cnpj_duckdb.py  –  versão estável

• Converte CSV Latin-1 → UTF-8 num cache local (1ª execução).
• Usa COPY … DATEFORMAT '%Y%m%d' NULL '00000000' para importar.
• Ajusta capital_social (vírgula → DOUBLE).
• Exporta Parquets (.zstd).

Requisitos:
    pip install duckdb==0.10.0 tqdm
"""

import os, re, shutil, tempfile
from pathlib import Path
import duckdb
from tqdm import tqdm


# ─────────── CONFIG ───────────
RAW_DIR     = Path(r"C:\Users\andrade\projetos\chat_empresas\data\unzipped_files_2023_05")
UTF8_DIR    = RAW_DIR.parent / "utf8_cache"
DB_PATH     = Path(r"C:\Users\andrade\projetos\chat_empresas\cnpj.duckdb")
PARQUET_DIR = RAW_DIR.parent / "parquet"
for d in (UTF8_DIR, PARQUET_DIR): d.mkdir(exist_ok=True, parents=True)
# ──────────────────────────────


def latin1_to_utf8(src: Path) -> Path:
    """Converte src (Latin-1) em UTF-8 na pasta cache, caso ainda não exista."""
    dst = UTF8_DIR / src.name
    if dst.exists():
        return dst
    print(f"⬆️  Convertendo → UTF-8: {src.name}")
    with src.open("rb") as fin, tempfile.NamedTemporaryFile("wb", delete=False) as tmp:
        for chunk in iter(lambda: fin.read(2 << 20), b""):            # 2 MiB
            tmp.write(chunk.decode("latin1").encode("utf-8"))
    shutil.move(tmp.name, dst)
    return dst


def connect_db() -> duckdb.DuckDBPyConnection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(f"PRAGMA threads={os.cpu_count()};")
    return con


def create_schema(con: duckdb.DuckDBPyConnection) -> None:
    ddl = r"""
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
    """
    for st in ddl.split(";"):
        s = st.strip()
        if s:
            con.execute(s + ";")


def load_files(con: duckdb.DuckDBPyConnection) -> None:
    mapping = [
        (r"EMPRECSV", "empresas", False),
        (r"ESTABELECSV", "estabelecimentos", True),
        (r"SOCIOCSV", "socios", True),
        (r"SIMPLES.*CSV", "simples", True),
        (r"CNAECSV", "cnaes", False),
        (r"NATJUCSV", "naturezas", False),
        (r"MUNICCSV", "municipios", False),
        (r"PAISCSV", "paises", False),
        (r"QUALSCSV", "qualificacoes", False),
        (r"MOTICCSV", "motivos", False),
    ]

    for raw in tqdm(sorted(RAW_DIR.iterdir()), desc="Importando"):
        fname = raw.name
        for regex, table, has_dates in mapping:
            if re.search(regex, fname, re.IGNORECASE):
                utf8 = latin1_to_utf8(raw)
                opts = "(DELIMITER ';', QUOTE '\"', HEADER FALSE, DATEFORMAT '%Y%m%d'"
                if has_dates:
                    opts += ", NULL '00000000'"
                opts += ")"
                con.execute(f"COPY {table} FROM '{utf8.as_posix()}' {opts};")
                if table == "empresas":
                    con.execute(
                        "UPDATE empresas "
                        "SET capital_social = REPLACE(capital_social, ',', '.')::DOUBLE "
                        "WHERE capital_social IS NOT NULL;"
                    )
                break
        else:
            print("⚠️  Ignorado:", fname)


def export_parquets(con: duckdb.DuckDBPyConnection) -> None:
    for tbl in (
        "empresas", "estabelecimentos", "socios", "simples",
        "cnaes", "naturezas", "municipios", "paises", "qualificacoes", "motivos"
    ):
        con.execute(
            f"COPY (SELECT * FROM {tbl}) "
            f"TO '{(PARQUET_DIR / (tbl + '.parquet')).as_posix()}' "
            "(FORMAT 'parquet', COMPRESSION 'zstd');"
        )


def main() -> None:
    con = connect_db()
    create_schema(con)
    load_files(con)
    export_parquets(con)
    print(f"\n✅ Banco pronto em {DB_PATH}")


if __name__ == "__main__":
    main()
