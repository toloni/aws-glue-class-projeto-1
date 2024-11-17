import logging
from typing import List
from utils.column_definitions import Base as BaseEnum

# Configuração do logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def valid_param_bases(args: List):
    try:
        list_bases = args["PARAM_BASES_TO_PROCESS"].split(",")

        for base in list_bases:
            if base not in BaseEnum.__members__:
                raise ValueError(f"Base com o nome '{base}' do Parâmetro é inválida.")

        return list_bases

    except Exception as e:
        logger.error(f"Erro: {str(e)}")
        raise


def valid_param_env(args: List):
    try:
        env = args["JOB_ENVIRONMENT"]

        if env not in ["dev", "hom", "prod"]:
            raise ValueError(f"Ambiente com o nome '{env}' do Parâmetro é inválida.")

        logger.info(f"Ambiente de execução {env} válido.")
        pass
    except Exception as e:
        logger.error(f"Erro: {str(e)}")
        raise
