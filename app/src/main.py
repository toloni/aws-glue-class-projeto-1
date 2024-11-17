import sys
import logging
from typing import Dict
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job

from app.utils.validations import valid_param_bases, valid_param_env
from utils.sub_modules.extract import extract
from utils.sub_modules.transform import transform_load

# Configuração do logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ===================================================================================== #
#                              -------=   MAIN   =-------                               #
# ===================================================================================== #
def main():

    try:
        args = getResolvedOptions(
            sys.argv,
            [
                "JOB_NAME",
                "JOB_ENVIRONMENT",
                "PARAM_BASES_TO_PROCESS",  #: "CNPJ9,CNPJ14,CARTEIRA,CONTA",
                "INPUT_MESH_DB_TABLE",
                "INPUT_S3_PATH_CACHE_CNPJ9",
                "INPUT_S3_PATH_CACHE_CNPJ14",
                "INPUT_S3_PATH_CACHE_CARTEIRA",
                "INPUT_S3_PATH_CACHE_CONTA",
                "OUTPUT_S3_PATH_DELTA_CNPJ9",
                "OUTPUT_S3_PATH_DELTA_CNPJ14",
                "OUTPUT_S3_PATH_DELTA_CARTEIRA",
                "OUTPUT_S3_PATH_DELTA_CONTA",
            ],
        )

        logger.info(f"Ambiente de execução: {args['JOB_ENVIRONMENT']}")
        valid_param_env(args)

        logger.info("Inicializando contexto do Spark e Glue")
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        job.init(args["JOB_NAME"], args)

        logger.info(f"Job iniciado: {args['JOB_NAME']}")
        logger.info(f"Bases para serem processadas: {args['PARAM_BASES_TO_PROCESS']}")
        args["PARAM_BASES_TO_PROCESS"] = valid_param_bases(args)

        # Extract
        logger.info("Iniciando etapa de extração")
        df_input_mesh, input_base_cache_dict = extract(args, glueContext, spark)

        # Transform >> Load
        logger.info("Iniciando etapa de transformação e carregamento")
        transform_load(df_input_mesh, input_base_cache_dict, args)

        logger.info("Commit do job em andamento")
        job.commit()

        logger.info("Job concluído com sucesso!")

    except Exception as e:
        print(f"Erro na orquestração dos trabalhos: {e}")


if __name__ == "__main__":
    main()
