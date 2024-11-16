import logging
from pyspark.sql import DataFrame
from utils.column_definitions import (
    Base,
)

# Configuração do logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# # ===================================================================================== #
# #                             -------=  EXTRACT   =-------                             #
# # ===================================================================================== #
# def extract(args, glueContext):

#     input_db_table = args["INPUT_MESH_DB_TABLE"].split(".")
#     banco_de_dados = input_db_table[0]
#     tabela = input_db_table[1]

#     logger.info(f"Lendo a última partição do DataMesh: {input_db_table}")
#     df_input_mesh = __get_last_partition_mesh(banco_de_dados, tabela, glueContext)

#     input_base_cache_dict = {}
#     input_path_cache_dict = {
#         Base.CNPJ9: args["INPUT_S3_PATH_CACHE_CNPJ9"],
#         Base.CNPJ14: args["INPUT_S3_PATH_CACHE_CNPJ14"],
#         Base.CARTEIRA: args["INPUT_S3_PATH_CACHE_CARTEIRA"],
#         Base.CONTA: args["INPUT_S3_PATH_CACHE_CONTA"],
#     }

#     for base in Base:
#         logger.info(
#             f"Carregando cache para a base {base.name} do caminho {input_path_cache_dict[base]}"
#         )
#         input_base_cache_dict[base] = __get_input_cache(
#             input_path_cache_dict[base], glueContext
#         )

#         logger.info(
#             f"Total de registros na base {base.name}: {input_base_cache_dict[base].count()}"
#         )

#     return df_input_mesh, input_base_cache_dict


# # -------------------------------------------------------------------------------------- #
# def __get_last_partition_mesh(banco_de_dados, tabela, glueContext) -> DataFrame:
#     logger.info(f"Lendo dados de: {banco_de_dados}.{tabela}")

#     try:
#         dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
#             database=banco_de_dados, table_name=tabela
#         )

#         return dynamic_frame.toDF()

#     except Exception as e:
#         logger.error(
#             f"Erro ao ler data catalog db:{banco_de_dados}, table: {tabela}: {e}"
#         )
#         raise


# # -------------------------------------------------------------------------------------- #
# def __get_input_cache(s3_path, glueContext) -> DataFrame:
#     logger.info(f"Lendo cache de entrada do caminho: {s3_path}")

#     try:
#         dynamic_frame = glueContext.create_dynamic_frame.from_options(
#             connection_type="s3",
#             connection_options={"paths": [s3_path]},
#             format="csv",
#             format_options={"withHeader": True, "separator": ",", "quoteChar": '"'},
#         )

#         return dynamic_frame.toDF()

#     except Exception as e:
#         logger.error(f"Erro ao ler o cache de entrada do caminho {s3_path}: {e}")
#         raise


# Cache Parquet
#
# def __get_input_cache(s3_path: str, glueContext) -> DataFrame:
#     """
#     Lê um arquivo Parquet do S3 e retorna como um DataFrame do Spark.

#     :param s3_path: Caminho no S3 do arquivo Parquet.
#     :param glueContext: Instância do GlueContext.
#     :return: DataFrame do Spark contendo os dados do arquivo.
#     """
#     logger.info(f"Lendo cache de entrada do caminho: {s3_path}")

#     # Leitura do arquivo Parquet
#     dynamic_frame = glueContext.create_dynamic_frame.from_options(
#         connection_type="s3",
#         connection_options={"paths": [s3_path]},
#         format="parquet"  # Especifica o formato Parquet
#     )

#     # Converte para Spark DataFrame e retorna
#     return dynamic_frame.toDF()


# ===================================================================================== #
#  local test                  -------=  EXTRACT   =-------                             #
# ===================================================================================== #
def extract(args, spark):

    input_db_table = args["INPUT_MESH_DB_TABLE"]
    logger.info(f"Lendo a última partição do DataMesh: {input_db_table}")
    df_input_mesh = __get_last_partition_mesh(input_db_table, spark)

    input_base_cache_dict = {}
    input_path_cache_dict = {
        Base.CNPJ9: args["INPUT_S3_PATH_CACHE_CNPJ9"],
        Base.CNPJ14: args["INPUT_S3_PATH_CACHE_CNPJ14"],
        Base.CARTEIRA: args["INPUT_S3_PATH_CACHE_CARTEIRA"],
        Base.CONTA: args["INPUT_S3_PATH_CACHE_CONTA"],
    }

    for base in Base:
        logger.info(
            f"Carregando cache para a base {base.name} do caminho {input_path_cache_dict[base]}"
        )
        input_base_cache_dict[base] = __get_input_cache(
            input_path_cache_dict[base], spark
        )

        logger.info(
            f"Total de registros na base {base.name}: {input_base_cache_dict[base].count()}"
        )

    return df_input_mesh, input_base_cache_dict


# -------------------------------------------------------------------------------------- #
def __get_last_partition_mesh(path_mesh, spark) -> DataFrame:
    logger.info(f"Lendo dados do caminho: {path_mesh}")
    df = spark.read.csv(path_mesh, header=True)
    return df


# -------------------------------------------------------------------------------------- #
def __get_input_cache(path_mesh, spark) -> DataFrame:
    logger.info(f"Lendo cache de entrada do caminho: {path_mesh}")
    df = spark.read.csv(path_mesh, header=True)
    return df
