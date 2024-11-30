from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, concat, coalesce


# Transform Single Base - Carteira
# ------------------------------------------------------------------------- #
def transform_carteira(
    df_encart_pj: DataFrame, df_cache: DataFrame, when_status
) -> DataFrame:

    carteira_columns = [
        "cod_hierarquia_gq_segmento",
        "cod_hierarquia_plataforma",
        "cod_hierarquia_gerente",
    ]

    # remover filiais
    df_encart_pj = df_encart_pj.select(
        *carteira_columns, "des_segmentacao", "cod_hierarquia_regiao"
    )

    # adicionar hash eliminar duplicados
    df_encart_pj = (
        df_encart_pj.dropDuplicates(carteira_columns)
        .withColumn(
            "lake_hash",
            sha2(
                concat(*carteira_columns),
                256,
            ),
        )
        .withColumn("lake_key", concat(*carteira_columns))
    )

    # preparar cache
    df_cache = df_cache.withColumnRenamed("hash", "cache_hash").withColumn(
        "cache_key",
        concat(
            "segmento",
            "plataforma",
            "numero",
        ),
    )

    df_joined = df_encart_pj.join(
        df_cache, df_encart_pj["lake_key"] == df_cache["cache_hash"], "full"
    )

    # realizar join e aplicar transformações
    return (
        df_joined.withColumns(when_status())
        .withColumns(
            {
                "cod_hierarquia_gq_segmento": coalesce(
                    col("cod_hierarquia_gq_segmento"), col("segmento")
                ),
                "cod_hierarquia_plataforma": coalesce(
                    col("cod_hierarquia_plataforma"), col("plataforma")
                ),
                "cod_hierarquia_gerente": coalesce(
                    col("cod_hierarquia_gerente"), col("numero")
                ),
                "hash": coalesce(col("lake_hash"), col("cache_hash")),
            }
        )
        .filter(col("status").isNotNull())
        .select(
            "id",
            "des_segmentacao",
            "cod_hierarquia_gq_segmento",
            "cod_hierarquia_plataforma",
            "cod_hierarquia_gerente",
            "cod_hierarquia_regiao",
            "hash",
            "status",
        )
    )
