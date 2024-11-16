import logging


# Configuração do logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ===================================================================================== #
#                                  ---=   LOAD   =---                                   #
# ===================================================================================== #
def load(df_transformed, path):
    logger.info(f"Escrevendo DataFrame no caminho: {path}")
    df_transformed.write.mode("append").parquet(path)
    logger.info(f"Dados salvos com sucesso no caminho: {path}")
