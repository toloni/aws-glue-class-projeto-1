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
from utils.extract import get_last_partition_mesh, get_cache_bases

from utils.load import load
from utils.transform import transform_cnpj9

start_time = time.time()
hora_atual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f"\n\nIniciando Job ..... {hora_atual} \n\n")


# Spark Session
spark = (
    SparkSession.builder.master("local[*]")
    .appName("Encarteiramento Delta")
    .getOrCreate()
)

# Input Mesh
path_mesh = "data//input//cliente.csv"

# Input Cache
path_cache = {}
path_cache[Base.CNPJ9] = "data//input//cache_cnpj9.csv"
path_cache[Base.CNPJ14] = "data//input//cache_cnpj14.csv"
path_cache[Base.CONTA] = "data//input//cache_conta.csv"
path_cache[Base.CARTEIRA] = "data//input//cache_carteira.csv"

# ============================================================================ #
#                                  EXTRACT                                     #
# ============================================================================ #
print("\n>> Extraindo Data Mesh .....\n")

# ler última particao
df_mesh = get_last_partition_mesh(path_mesh, spark)
print(f"Total de Registrso Mesh .... : {df_mesh.count()}")

print("\n>> Extraindo Cache .........\n")

# ler cache
input_cache = {}

for base in Base:
    input_cache[base] = get_cache_bases(path_cache[base], spark)

for base in Base:
    print(f"Total de Registrso {base.name} .... : {input_cache[base].count()}")
#
#
# ============================================================================ #
#                            TRANSFORM  >>  LOAD                               #
# ============================================================================ #
print("\nTransformando dados .......\n")

df_delta = {}

df_delta = transform_cnpj9(df_mesh, input_cache[Base.CNPJ9], Base.CNPJ9.name)
print(">> Delta:")
df_delta.show()


#

# df_delta_cnpj14 = transform(partitions_mesh_df, partitions_dates, Base.CNPJ14.value)
# df_delta_conta = transform(partitions_mesh_df, partitions_dates, Base.CONTA.value)
# df_delta_carteira = transform(partitions_mesh_df, partitions_dates, Base.CARTEIRA.value)

# ============================================================================ #
#                                   LOAD                                       #
# ============================================================================ #
print("\nCarregando Dados ........\n")

# load(df_delta_cnpj9, Base.CNPJ9.value, path_delta_cnpj9)
# load(df_delta_cnpj14, Base.CNPJ14.value, path_delta_cnpj14)
# load(df_delta_conta, Base.CONTA.value, path_delta_conta)
# load(df_delta_carteira, Base.CARTEIRA.value, path_delta_carteira)


# ============================================================================ #
#                                  FINALIZAR                                   #
# ============================================================================ #
end_time = time.time()
execution_time = end_time - start_time
hora_atual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f"\n\nFinalizando Job ..... {hora_atual} ")
print(f"Tempo de execução ...: {execution_time:.5f} segundos\n\n")
