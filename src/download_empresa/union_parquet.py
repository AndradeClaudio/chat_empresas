import pandas as pd
import os

# Caminho onde estão os arquivos Parquet
pasta_arquivos = './data/parquet'
arquivos_parquet = [os.path.join(pasta_arquivos, f) for f in os.listdir(pasta_arquivos) if f.endswith('.parquet')]

# Lista para armazenar os DataFrames
dataframes = []

# Ler todos os arquivos Parquet
for arquivo in arquivos_parquet:
    df = pd.read_parquet(arquivo)
    dataframes.append(df)

# Concatenar todos os DataFrames
df_unificado = pd.concat(dataframes, ignore_index=True)

# Salvar em um único arquivo Parquet
caminho_saida = './data/parquet/empresas.parquet'
df_unificado.to_parquet(caminho_saida)

print(f"Arquivo unificado salvo em: {caminho_saida}")
