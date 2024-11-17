import logging
from utils.column_definitions import Base as BaseEnum

# Configuração do logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def valid_param_bases(bases: str):
    try:
        list_bases = bases.split(",")

        for base in list_bases:
            if base not in BaseEnum.__members__:
                raise ValueError(f"Base com o nome '{base}' do Parâmetro é inválida.")

        return list_bases

    except Exception as e:
        logger.error(f"Erro: {str(e)}")
        raise
