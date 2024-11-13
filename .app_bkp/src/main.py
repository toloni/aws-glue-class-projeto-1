# TODO
# 1 - Plugar no DataMesh
# 2 - Plugar Local de Destino Delta
# 3 - Testes Unitarios de Load, Extract e Main
# 4 - Incluir Logs de Observability
# 5 - Excluir Delta Anterior

import time
from pyspark.sql import SparkSession
from datetime import datetime
from utils.dates_to_process import DateToProcess
from utils.enum import Base
from utils.extract import (
    get_last_date_delta,
    get_last_partition_date,
    get_prev_partition_date,
    read_partitions_mesh,
)

from utils.load import load
from utils.transform import transform

start_time = time.time()
hora_atual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f"\n\nIniciando Job ..... {hora_atual} \n\n")


# Spark Session
spark = (
    SparkSession.builder.master("local[*]")
    .appName("Encarteiramento Delta")
    .getOrCreate()
)

# Parametros de Entrada
path_mesh = "data//input//cliente.csv"
path_delta_cnpj9 = "data//output//delta_cnpj9"
path_delta_cnpj14 = "data//output//delta_cnpj14"
path_delta_conta = "data//output//delta_conta"
path_delta_carteira = "data//output//delta_carteira"


def process_dates(partitions_dates, spark, path, base, last_date_mesh, prev_date_mesh):
    last_date_delta = get_last_date_delta(spark, path)
    date_to_add = last_date_delta if last_date_delta is not None else prev_date_mesh
    partitions_dates.add_dates(base, last_date_mesh, date_to_add)


# ============================================================================ #
#                                  EXTRACT                                     #
# ============================================================================ #
print("\nExtraindo Data Mesh .....\n")

# Ler datas das partições do Data Mesh
last_date_mesh = get_last_partition_date(spark, path_mesh)
prev_date_mesh = get_prev_partition_date(spark, path_mesh, last_date_mesh)

# Datas das Partições
partitions_dates = DateToProcess()

# Lista de caminhos e bases
paths_and_bases = [
    (path_delta_cnpj9, Base.CNPJ9.value),
    (path_delta_cnpj14, Base.CNPJ14.value),
    (path_delta_conta, Base.CONTA.value),
    (path_delta_carteira, Base.CARTEIRA.value),
]

# Obter datas de processamento por Base
for path, base in paths_and_bases:
    process_dates(partitions_dates, spark, path, base, last_date_mesh, prev_date_mesh)

# Partições do Data Mesh
partitions_mesh_df = read_partitions_mesh(
    spark, partitions_dates.get_unique_dates(), path_mesh
)

for date in partitions_mesh_df:
    print(f"Partição {date} .......: {partitions_mesh_df[date].count()}")

# ============================================================================ #
#                                TRANSFORM                                     #
# ============================================================================ #
print("\nTransformando dados .......")

df_delta_cnpj9 = transform(partitions_mesh_df, partitions_dates, Base.CNPJ9.value)
df_delta_cnpj14 = transform(partitions_mesh_df, partitions_dates, Base.CNPJ14.value)
df_delta_conta = transform(partitions_mesh_df, partitions_dates, Base.CONTA.value)
df_delta_carteira = transform(partitions_mesh_df, partitions_dates, Base.CARTEIRA.value)

# ============================================================================ #
#                                   LOAD                                       #
# ============================================================================ #
print("\nCarregando Dados ........\n")

load(df_delta_cnpj9, Base.CNPJ9.value, path_delta_cnpj9)
load(df_delta_cnpj14, Base.CNPJ14.value, path_delta_cnpj14)
load(df_delta_conta, Base.CONTA.value, path_delta_conta)
load(df_delta_carteira, Base.CARTEIRA.value, path_delta_carteira)


# ============================================================================ #
#                                  FINALIZAR                                   #
# ============================================================================ #
end_time = time.time()
execution_time = end_time - start_time
hora_atual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f"\n\nFinalizando Job ..... {hora_atual} ")
print(f"Tempo de execução ...: {execution_time:.5f} segundos\n\n")
