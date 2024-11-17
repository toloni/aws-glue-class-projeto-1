# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from awsglue.context import GlueContext
# from awsglue.job import Job
import sys
import logging
from typing import Dict
from pyspark.sql import SparkSession

from utils.validations import valid_param_bases, valid_param_env
from utils.sub_modules.extract import extract
from utils.sub_modules.transform import transform_load

# Configuração do logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# # # Local Param # # # #
def getResolvedOptions(args, params):
    """Transforma argumentos do tipo --CHAVE VALOR em um dicionário."""
    params = {}
    i = 1  # Ignorar sys.argv[0], que é o nome do script
    while i < len(args):
        if args[i].startswith("--"):
            key = args[i][2:]  # Remove os dois traços '--'
            if i + 1 < len(args) and not args[i + 1].startswith("--"):
                params[key] = args[i + 1]
                i += 2  # Avança para o próximo par
            else:
                params[key] = None  # Caso a chave não tenha valor
                i += 1
        else:
            i += 1
    return params


# # #  # # #  # # #


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
        glueContext = None
        # # #

        logger.info(f"Job iniciado: {args['JOB_NAME']}")
        logger.info(f"Bases para serem processadas: {args['PARAM_BASES_TO_PROCESS']}")
        args["PARAM_BASES_TO_PROCESS"] = valid_param_bases(args)

        logger.info("Iniciando etapa de extração")
        df_input_mesh, input_base_cache_dict = extract(args, glueContext, spark)

        logger.info("Persistindo mesh em memória")
        df_input_mesh_cached = df_input_mesh.persist()

        logger.info("Iniciando etapa de transformação e carregamento")
        transform_load(df_input_mesh_cached, input_base_cache_dict, args)

        logger.info("Liberando memória")
        df_input_mesh_cached.unpersist()

        logger.info("Commit do job em andamento")
        # # #
        # job.commit()
        # # #

        logger.info("Job concluído com sucesso!")

    except Exception as e:
        print(f"Erro na execução do Job, erro: {e}")


if __name__ == "__main__":
    main()
