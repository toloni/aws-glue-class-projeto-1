import logging
from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


# Configuração do logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ===================================================================================== #
#                                  ---=   LOAD   =---                                   #
# ===================================================================================== #
def load(df_transformed: DataFrame, path: str, range_plat_dict: Dict):
    logger.info(f"Escrevendo DataFrame no caminho: {path}")

    df_filtred = df_transformed.filter(
        (col("plat") >= range_plat_dict["start"])
        & (col("plat") <= range_plat_dict["end"])
    )

    df_filtred.show(truncate=False)

    df_filtred.write.mode("append").parquet(path)

    logger.info(f"Dados salvos com sucesso no caminho: {path}")
