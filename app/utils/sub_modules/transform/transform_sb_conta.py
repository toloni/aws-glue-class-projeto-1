from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, concat, coalesce


# Transform Single Base - Conta Bancaria
# ------------------------------------------------------------------------- #
def transform_conta(
    df_encart_pj: DataFrame, df_cache: DataFrame, when_status
) -> DataFrame:

    # remover filiais
    df_encart_pj = df_encart_pj.select(
        "des_segmentacao",
        "num_agencia",
        "num_conta",
        "num_conta_dac",
        "des_conta_status",
    )

    # adicionar hash eliminar duplicados
    df_encart_pj = (
        df_encart_pj.withColumn(
            "lake_hash",
            sha2(
                concat(
                    "des_segmentacao",
                    "des_conta_status",
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

    # preparar cache
    df_cache = (
        df_cache.withColumn("agencia", df_cache["agencia"].cast("int"))
        .withColumn("numeroconta", df_cache["numeroconta"].cast("int"))
        .withColumnRenamed("hash", "cache_hash")
    )

    # cache_key
    df_cache = df_cache.withColumn(
        "cache_key", concat(col("agencia"), col("numeroconta"))
    )

    df_joined = df_encart_pj.join(
        df_cache, df_encart_pj["lake_key"] == df_cache["cache_key"], "full"
    )

    # realizar join e aplicar transformações
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
