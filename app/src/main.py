import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from utils.enums import Base
from utils.sub_modules.extract.extract import Extractor
from utils.sub_modules.transform.transform import Transformer
from utils.sub_modules.load.load import load
from utils.validations import is_to_process_base
from utils.logging_utils import ProcessingLogger

# Configurações do CloudWatch Logs
LOG_GROUP_NAME = "/aws/glue/etl-processing"
LOG_STREAM_NAME = "etl-stats-stream"


class ETL:
    def __init__(self, args, glueContext, df_encart_pj, logger: ProcessingLogger):
        self.args = args
        self.glueContext = glueContext
        self.df_encart_pj = df_encart_pj
        self.logger = logger

    def run(self, base):
        self.logger.log_info(f"Iniciando processamento para a base: {base.name}")
        if not is_to_process_base(self.args, base):
            self.logger.log_info(
                f"Base {base.name} não está marcada para processamento. Pulando."
            )
            return

        try:
            # EXTRACT #
            self.logger.log_info(f"Extraindo dados de cache para a base {base.name}")
            extractor = Extractor(args=self.args, glue_context=self.glueContext)
            df_cache_dict = extractor.extract_cache(base=base)

            # TRANSFORM #
            self.logger.log_info(f"Transformando dados para a base {base.name}")
            transformer = Transformer()
            df_transformed_data = transformer.transform(
                self.df_encart_pj, df_cache_dict, base
            )

            # Log de estatísticas
            self.logger.log_status_count(df_transformed_data, base.name)

            # LOAD #
            self.logger.log_info(f"Carregando dados para a base {base.name}")
            load(df_transformed_data, base)

            self.logger.log_info(
                f"Processamento concluído com sucesso para a base {base.name}"
            )

        except Exception as e:
            self.logger.log_error(f"Erro ao processar a base {base.name}: {str(e)}")
            sys.exit(1)


def main():
    logger = ProcessingLogger(log_group=LOG_GROUP_NAME, log_stream=LOG_STREAM_NAME)
    logger.log_info("Iniciando job de ETL")

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

        logger.log_info("Inicializando GlueContext e SparkContext")
        sc = SparkContext()
        glueContext = GlueContext(sc)
        job = Job(glueContext)
        job.init(args["JOB_NAME"], args)

        logger.log_info("Extraindo dados da tabela principal")
        extractor = Extractor(args=args, glue_context=glueContext)
        df_encart_pj = extractor.extract_lake()
        etl = ETL(args, glueContext, df_encart_pj, logger)

        for base in Base:
            etl.run(base)

        logger.log_info("Commit no job do Glue")
        job.commit()

        logger.log_info("Job de ETL concluído com sucesso")

    except Exception as e:
        logger.log_error(f"Erro no job de ETL: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
