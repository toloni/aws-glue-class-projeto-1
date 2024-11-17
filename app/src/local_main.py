# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from awsglue.context import GlueContext
# from awsglue.job import Job
import sys
import logging
from typing import Dict
from pyspark.sql import SparkSession

from utils.validations import valid_param_bases
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
        # # #
        # args = getResolvedOptions(
        #     sys.argv,
        #     [
        #         "JOB_NAME",
        #         "INPUT_MESH_DB_TABLE",
        #         "INPUT_S3_PATH_CACHE_CNPJ9",
        #         "INPUT_S3_PATH_CACHE_CNPJ14",
        #         "INPUT_S3_PATH_CACHE_CARTEIRA",
        #         "INPUT_S3_PATH_CACHE_CONTA",
        #         "OUTPUT_S3_PATH_DELTA_CNPJ9",
        #         "OUTPUT_S3_PATH_DELTA_CNPJ14",
        #         "OUTPUT_S3_PATH_DELTA_CARTEIRA",
        #         "OUTPUT_S3_PATH_DELTA_CONTA",
        #     ],
        # )
        # # #
        args = {
            "JOB_NAME": "LocalJob",
            "JOB_ENVIRONMENT": "dev",
            "PARAM_BASES_TO_PROCESS": "CNPJ9,CNPJ14,CARTEIRA,CONTA",
            "INPUT_MESH_DB_TABLE": "data//input//cliente.csv",
            "INPUT_S3_PATH_CACHE_CNPJ9": "data//input//cache_cnpj9.csv",
            "INPUT_S3_PATH_CACHE_CNPJ14": "data//input//cache_cnpj14.csv",
            "INPUT_S3_PATH_CACHE_CARTEIRA": "data//input//cache_carteira.csv",
            "INPUT_S3_PATH_CACHE_CONTA": "data//input//cache_conta.csv",
            "OUTPUT_S3_PATH_DELTA_CNPJ9": "data//output//delta_cnpj9",
            "OUTPUT_S3_PATH_DELTA_CNPJ14": "data//output//delta_cnpj14",
            "OUTPUT_S3_PATH_DELTA_CARTEIRA": "data//output//delta_carteira",
            "OUTPUT_S3_PATH_DELTA_CONTA": "data//output//delta_conta",
        }
        # # #

        logger.info("Inicializando contexto do Spark e Glue")
        # # #
        # sc = SparkContext()
        # glueContext = GlueContext(sc)
        # spark = glueContext.spark_session
        # job = Job(glueContext)
        # job.init(args["JOB_NAME"], args)
        # # #
        spark = (
            SparkSession.builder.master("local[*]")
            .appName("Encarteiramento Delta")
            .getOrCreate()
        )
        # # #

        logger.info(f"Job iniciado: {args['JOB_NAME']}")
        logger.info(f"Bases para serem processadas: {args['PARAM_BASES_TO_PROCESS']}")
        args["PARAM_BASES_TO_PROCESS"] = valid_param_bases(
            args["PARAM_BASES_TO_PROCESS"]
        )

        # Extract
        logger.info("Iniciando etapa de extração")
        df_input_mesh, input_base_cache_dict = extract(args, spark)

        # Transform >> Load
        logger.info("Iniciando etapa de transformação e carregamento")
        transform_load(df_input_mesh, input_base_cache_dict, args)

        logger.info("Commit do job em andamento")
        # # #
        # job.commit()
        # # #

        logger.info("Job concluído com sucesso!")

    except Exception as e:
        print(f"Erro na execução do Job, erro: {e}")


if __name__ == "__main__":
    main()
