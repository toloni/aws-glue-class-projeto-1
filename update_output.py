import os
import shutil

# Caminho do diretório Parquet
caminho_diretorio = "data/output/"

arqs = ["delta_cnpj9", "delta_cnpj14", "delta_conta", "delta_carteira"]

for arq in arqs:
    path = caminho_diretorio + arq
    # Remove o diretório Parquet e seu conteúdo
    if os.path.exists(path):
        shutil.rmtree(path)
        print(f"Diretório {path} deletado com sucesso.")
    else:
        print(f"O diretório {path} não existe.")
