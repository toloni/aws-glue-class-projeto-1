from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when
from utils.enums import Base, Status
from utils.sub_modules.transform.transform_sb_cnpj9 import transform_cnpj9
from utils.sub_modules.transform.transform_sb_cnpj14 import transform_cnpj14
from utils.sub_modules.transform.transform_sb_carteira import transform_carteira
from utils.sub_modules.transform.transform_sb_conta import transform_conta
from typing import Dict


class Transformer:
    def __init__(self):
        """
        Inicializa a classe Transformer, responsável por gerenciar e aplicar transformações
        de bases de dados específicas.
        """
        self.status_col = self.define_status_column

    @staticmethod
    def define_status_column() -> Dict:
        """Define a coluna de status com base na comparação de hashes."""
        return {
            "status": when(col("cache_hash").isNull(), lit(Status.INSERT.value))
            .when(col("lake_hash").isNull(), lit(Status.DELETE.value))
            .when(col("lake_hash") != col("cache_hash"), lit(Status.UPDATE.value))
        }

    def transform(
        self, df_encart_pj: DataFrame, df_cache_dict: Dict, base: Base
    ) -> DataFrame:
        """
        Aplica a transformação apropriada com base no tipo de base fornecido.

        Args:
            df_encart_pj (DataFrame): DataFrame contendo os dados do encarte PJ.
            df_cache_dict (Dict): Dicionário contendo os DataFrames de cache por base.
            base (Base): Tipo da base a ser transformada.

        Returns:
            DataFrame: DataFrame transformado.
        """

        if base == Base.CNPJ9:
            return transform_cnpj9(
                df_encart_pj, df_cache_dict[Base.CNPJ9], self.status_col
            )

        elif base == Base.CNPJ14:
            return transform_cnpj14(
                df_encart_pj, df_cache_dict[Base.CNPJ14], self.status_col
            )

        elif base == Base.CARTEIRA:
            return transform_carteira(
                df_encart_pj, df_cache_dict[Base.CARTEIRA], self.status_col
            )

        elif base == Base.CONTA:
            return transform_conta(
                df_encart_pj, df_cache_dict[Base.CONTA], self.status_col
            )

        raise ValueError(f"Base desconhecida: {base}")
