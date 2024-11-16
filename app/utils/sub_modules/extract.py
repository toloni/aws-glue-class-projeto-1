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


# ===================================================================================== #
#                             -------=  EXTRACT   =-------                             #
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
