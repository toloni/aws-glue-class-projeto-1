from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, concat, coalesce, when, lit
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

    def prepare_encart_pj(df: DataFrame) -> DataFrame:

        chave_conta = [
            "num_agencia",
            "num_conta",
            "num_conta_dac",
        ]

        df = df.select(
            "cod_hierarquia_gq_segmento",
            "des_segmentacao",
            "id_chave_conta",
            "num_agencia",
            "num_conta",
            "num_conta_dac",
            "id_chave_cliente",
            "dat_inicio_relacionamento_harmonizado",
            "cod_conta_gestora",
        )

        return (
            df.withColumn("lake_key", concat(*chave_conta))
            .withColumn(
                "contadac",
                concat("num_conta", "num_conta_dac"),
            )
            .withColumn(
                "lake_hash",
                sha2(
                    concat(
                        "cod_hierarquia_gq_segmento",
                        "des_segmentacao",
                        "id_chave_cliente",
                        "cod_conta_gestora",
                    ),
                    256,
                ),
            )
        )

    def prepare_cache(df: DataFrame) -> DataFrame:
        df = df.withColumns(
            {
                "agencia": col("agencia").cast("int"),
                "numeroconta": col("numeroconta").cast("int"),
            }
        ).select(
            "id",
            "agencia",
            "numeroconta",
            "hash",
        )

        return df.withColumnRenamed("hash", "cache_hash").withColumn(
            "cache_key", concat(col("agencia"), col("numeroconta"))
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
                "agencia": coalesce(col("num_agencia"), col("agencia")),
                "numeroconta": coalesce(col("contadac"), col("numeroconta")),
                "hash": coalesce(col("lake_hash"), col("cache_hash")),
            }
        )
        .filter(col("status").isNotNull())
        .select(
            "id_chave_conta",
            "agencia",
            "numeroconta",
            "cod_hierarquia_gq_segmento",
            "des_segmentacao",
            "dat_inicio_relacionamento_harmonizado",
            "cod_conta_gestora",
            "hash",
            "status",
        )
    )
