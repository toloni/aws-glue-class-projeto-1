import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from utils.enums import Base
from utils.sub_modules.extract.extract import Extractor
from utils.sub_modules.transform.transform import Transformer
from utils.sub_modules.load.load import load

from utils.validations import is_to_process_base


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


class ETL:
    def __init__(self, args, glueContext, df_encart_pj):
        self.args = args
        self.glueContext = glueContext
        self.df_encart_pj = df_encart_pj

    def run(self, base):
        logger.info(f"Iniciando processamento para a base: {base.name}")
        if not is_to_process_base(self.args, base):
            logger.info(
                f"Base {base.name} não está marcada para processamento. Pulando."
            )
            return

        try:
            # EXTRACT #
            logger.info(f"Extraindo dados de cache para a base {base.name}")
            extractor = Extractor(args=self.args, glue_context=self.glueContext)
            df_cache_dict = extractor.extract_cache(base=base)

            # TRANSFORM #
            logger.info(f"Transformando dados para a base {base.name}")
            transformer = Transformer()
            df_transformed_data = transformer.transform(
                self.df_encart_pj, df_cache_dict, base
            )

            # LOAD #
            logger.info(f"Carregando dados para a base {base.name}")
            load(df_transformed_data, base)

            logger.info(f"Processamento concluído com sucesso para a base {base.name}")

        except Exception as e:
            logger.error(
                f"Erro ao processar a base {base.name}: {str(e)}", exc_info=True
            )
            sys.exit(1)


def main():
    logger.info("Iniciando job de ETL")

    try:
        args = getResolvedOptions(
            sys.argv,
            [
                "JOB_NAME",
                "INPUT_DB_TABLE",
                "PATH_S3_CNPJ9",
                "PATH_S3_CNPJ14",
                "PATH_S3_CARTEIRA",
                "PATH_S3_CONTA",
                "BASES_TO_PROCESS",
            ],
        )

        logger.info("Inicializando GlueContext e SparkContext")
        sc = SparkContext()
        glueContext = GlueContext(sc)
        job = Job(glueContext)
        job.init(args["JOB_NAME"], args)

        logger.info("Extraindo dados da tabela principal")
        extractor = Extractor(args=args, glue_context=glueContext)
        df_encart_pj = extractor.extract_lake()
        etl = ETL(args, glueContext, df_encart_pj)

        for base in Base:
            etl.run(base)

        logger.info("Commit no job do Glue")
        job.commit()
        logger.info("Job de ETL concluído com sucesso")

    except Exception as e:
        logger.error(f"Erro no job de ETL: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
