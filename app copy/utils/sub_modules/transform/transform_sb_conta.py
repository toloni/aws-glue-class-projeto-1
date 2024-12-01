from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, concat, coalesce
from typing import Dict, Callable


def transform_conta(
    df_encart_pj: DataFrame,
    df_cache: DataFrame,
    when_status: Callable[[], Dict[str, DataFrame]],
) -> DataFrame:
    """
    Transforma a base de dados de contas bancárias unindo dados do encarte PJ e cache.

    Args:
        df_encart_pj (DataFrame): DataFrame contendo os dados do encarte PJ.
        df_cache (DataFrame): DataFrame contendo os dados do cache.
        when_status (function): Função que retorna as colunas de status com base em condições.

    Returns:
        DataFrame: DataFrame resultante após a transformação.
    """

    # Função auxiliar para preparar o DataFrame do encarte PJ
    def prepare_encart_pj(df: DataFrame) -> DataFrame:
        return (
            df.select(
                "des_segmentacao",
                "num_agencia",
                "num_conta",
                "num_conta_dac",
                "des_conta_status",
            )
            .withColumn(
                "lake_hash",
                sha2(
                    concat(
                        col("des_segmentacao"),
                        col("des_conta_status"),
                    ),
                    256,
                ),
            )
            .withColumn(
                "lake_key",
                concat(col("num_agencia"), col("num_conta"), col("num_conta_dac")),
            )
            .withColumn("contadac", concat(col("num_conta"), col("num_conta_dac")))
        )

    # Função auxiliar para preparar o DataFrame de cache
    def prepare_cache(df: DataFrame) -> DataFrame:
        return (
            df.withColumn("agencia", df["agencia"].cast("int"))
            .withColumn("numeroconta", df["numeroconta"].cast("int"))
            .withColumnRenamed("hash", "cache_hash")
            .withColumn("cache_key", concat(col("agencia"), col("numeroconta")))
        )

    # Preparação dos DataFrames
    df_encart_pj = prepare_encart_pj(df_encart_pj)
    df_cache = prepare_cache(df_cache)

    # Realizar o join entre encarte PJ e cache
    df_joined = df_encart_pj.join(
        df_cache, df_encart_pj["lake_key"] == df_cache["cache_key"], "full"
    )

    # Aplicar transformações e retorno final
    return (
        df_joined.withColumns(when_status())
        .withColumns(
            {
                "num_agencia": coalesce(col("num_agencia"), col("agencia")),
                "numeroconta": coalesce(col("contadac"), col("numeroconta")),
                "hash": coalesce(col("lake_hash"), col("cache_hash")),
            }
        )
        .filter(col("status").isNotNull())
        .select(
            "id",
            "des_segmentacao",
            "num_agencia",
            "numeroconta",
            "des_conta_status",
            "hash",
            "status",
        )
    )
