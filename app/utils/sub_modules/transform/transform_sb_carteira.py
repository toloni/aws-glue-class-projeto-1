from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, concat, coalesce


# Transformação da base única - Carteira
def transform_carteira(
    df_encart_pj: DataFrame, df_cache: DataFrame, when_status
) -> DataFrame:
    """
    Realiza a transformação da base Carteira unindo dados do encarte PJ e cache.

    Args:
        df_encart_pj (DataFrame): DataFrame contendo os dados do encarte PJ.
        df_cache (DataFrame): DataFrame contendo os dados do cache.
        when_status (function): Função que retorna as colunas de status com base em condições.

    Returns:
        DataFrame: DataFrame resultante após a transformação.
    """
    carteira_columns = [
        "cod_hierarquia_gq_segmento",
        "cod_hierarquia_plataforma",
        "cod_hierarquia_gerente",
    ]

    def prepare_encart_pj(df: DataFrame) -> DataFrame:
        """Prepara o DataFrame do encarte PJ filtrando e adicionando hash e chave."""
        df = df.select(*carteira_columns, "des_segmentacao", "cod_hierarquia_regiao")
        return (
            df.dropDuplicates(carteira_columns)
            .withColumn(
                "lake_hash",
                sha2(concat(*carteira_columns, "cod_hierarquia_regiao"), 256),
            )
            .withColumn("lake_key", concat(*carteira_columns))
        )

    def prepare_cache(df: DataFrame) -> DataFrame:
        """Prepara o DataFrame de cache ajustando nomes e criando chave."""
        return df.withColumnRenamed("hash", "cache_hash").withColumn(
            "cache_key",
            concat(
                col("segmento"),
                col("plataforma"),
                col("numero"),
            ),
        )

    # Preparo dos dados
    df_encart_pj = prepare_encart_pj(df_encart_pj)
    df_cache = prepare_cache(df_cache)

    # União dos DataFrames
    df_joined = df_encart_pj.join(
        df_cache, df_encart_pj["lake_key"] == df_cache["cache_key"], "full"
    )

    # Transformação final
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
