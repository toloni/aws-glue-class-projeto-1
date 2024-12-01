from awsglue.context import GlueContext
from pyspark.sql import DataFrame

from typing import Dict

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def extract_lake(args: Dict, glueContext: GlueContext) -> DataFrame:

    banco_de_dados, tabela = args["INPUT_DB_TABLE"].split(".")
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
