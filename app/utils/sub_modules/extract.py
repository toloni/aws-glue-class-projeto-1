import logging
from pyspark.sql import DataFrame, SparkSession
from utils.column_definitions import (
    Base as BaseEnum,
)

# Configuração do logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ===================================================================================== #
#                             -------=  EXTRACT   =-------                              #
# ===================================================================================== #
def extract(args, glueContext, spark: SparkSession):

    job_env = args["JOB_ENVIRONMENT"]
    banco_de_dados, tabela = args["INPUT_MESH_DB_TABLE"].split(".")
    bases_to_process = args["PARAM_BASES_TO_PROCESS"]

    logger.info(f"Lendo a última partição do DataMesh: {banco_de_dados}.{tabela}")

    df_input_mesh = (
        __get_last_partition_mesh(banco_de_dados, tabela, glueContext)
        if (job_env == "prod")
        else __get_mesh_dev_hom(banco_de_dados, tabela, glueContext, spark, job_env)
    )

    if df_input_mesh is None:
        raise ValueError("Base Mesh Inválida.")

    input_base_cache_dict = {}
    input_path_cache_dict = __build_input_path_cache_dict(args)

    for base_param in bases_to_process:
        base = BaseEnum[base_param]
        logger.info(
            f"Carregando cache para a base {base.name} do caminho {input_path_cache_dict[base]}"
        )

        input_base_cache_dict[base] = __get_input_cache(
            input_path_cache_dict[base], spark, job_env
        )

        logger.info(
            f"Total de registros na base {base.name}: {input_base_cache_dict[base].count()}"
        )

    return df_input_mesh, input_base_cache_dict


# -------------------------------------------------------------------------------------- #
def __build_input_path_cache_dict(args):
    """Build the input path cache dictionary from the arguments."""
    return {
        BaseEnum.CNPJ9: args["INPUT_S3_PATH_CACHE_CNPJ9"],
        BaseEnum.CNPJ14: args["INPUT_S3_PATH_CACHE_CNPJ14"],
        BaseEnum.CARTEIRA: args["INPUT_S3_PATH_CACHE_CARTEIRA"],
        BaseEnum.CONTA: args["INPUT_S3_PATH_CACHE_CONTA"],
    }


# -------------------------------------------------------------------------------------- #
def __get_last_partition_mesh(banco_de_dados, tabela, glueContext) -> DataFrame:
    logger.info(f"Lendo dados de: {banco_de_dados}.{tabela}")

    try:
        # Utilizar datautils
        dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database=banco_de_dados, table_name=tabela
        )

        return dynamic_frame.toDF()

    except Exception as e:
        logger.error(
            f"Erro ao ler data catalog db:{banco_de_dados}, table: {tabela}: {e}"
        )
        raise


# -------------------------------------------------------------------------------------- #
def __get_mesh_dev_hom(
    banco_de_dados: str, tabela: str, glueContext, spark: SparkSession, env: str
) -> DataFrame:

    try:
        if env == "local":
            # local csv
            path = banco_de_dados + "." + tabela
            return spark.read.csv(path, header=True)
        else:
            # Data Catalog Nao Particionado
            dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
                database=banco_de_dados, table_name=tabela
            )
            return dynamic_frame.toDF()

    except Exception as e:
        logger.error(
            f"Erro ao ler data catalog db:{banco_de_dados}, table: {tabela}: {e}"
        )

    return None


# -------------------------------------------------------------------------------------- #
def __get_input_cache(s3_path: str, spark: SparkSession, env_job: str) -> DataFrame:
    """
    Lê um arquivo Parquet do S3 e retorna como um DataFrame do Spark.
    Caso ocorra algum erro, uma mensagem de erro será registrada.

    :param s3_path: Caminho no S3 do arquivo Parquet.
    :param spark: Instância do SparkSession.
    :return: DataFrame do Spark contendo os dados do arquivo, ou None em caso de erro.
    """
    try:
        logger.info(f"Lendo cache de entrada do caminho: {s3_path}")
        if env_job == "local":
            return spark.read.csv(s3_path, header=True)

        return spark.read.parquet(s3_path)

    except Exception as e:
        logger.error(
            f"Erro ao ler o arquivo Parquet do S3. Caminho: {s3_path}. Erro: {str(e)}"
        )
        return None
