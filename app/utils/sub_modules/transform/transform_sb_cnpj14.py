from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, concat, coalesce


# Transformação da base única - CNPJ14
def transform_cnpj14(
    df_encart_pj: DataFrame, df_cache: DataFrame, when_status
) -> DataFrame:
    """
    Realiza a transformação da base CNPJ14 unindo dados do encarte PJ e cache.

    Args:
        df_encart_pj (DataFrame): DataFrame contendo os dados do encarte PJ.
        df_cache (DataFrame): DataFrame contendo os dados do cache.
        when_status (function): Função que retorna as colunas de status com base em condições.

    Returns:
        DataFrame: DataFrame resultante após a transformação.
    """

    def prepare_encart_pj(df: DataFrame) -> DataFrame:
        """Prepara o DataFrame do encarte PJ filtrando e adicionando hash."""
        df = df.select(
            "num_cpfcnpj",
            "num_cpfcnpj14",
            "id_chave_cliente",
            "des_nome_cliente_razao_social",
            "des_cpfcnpj14_status",
        )
        return df.dropDuplicates(["num_cpfcnpj14"]).withColumn(
            "lake_hash",
            sha2(
                concat(
                    col("des_nome_cliente_razao_social"),
                    col("des_cpfcnpj14_status"),
                ),
                256,
            ),
        )

    def prepare_cache(df: DataFrame) -> DataFrame:
        """Prepara o DataFrame de cache ajustando nomes de colunas."""
        return df.withColumnRenamed("hash", "cache_hash")

    # Preparo dos dados
    df_encart_pj = prepare_encart_pj(df_encart_pj)
    df_cache = prepare_cache(df_cache)

    # União dos DataFrames
    df_joined = df_encart_pj.join(
        df_cache, df_encart_pj["num_cpfcnpj14"] == df_cache["cnpj"], "full"
    )

    # Transformação final
    return (
        df_joined.withColumns(when_status())
        .withColumns(
            {
                "id_chave_cliente": coalesce(col("id_chave_cliente"), col("id")),
                "num_cpfcnpj14": coalesce(col("num_cpfcnpj14"), col("cnpj")),
                "num_cpfcnpj": coalesce(col("num_cpfcnpj"), col("cnpj9")),
                "hash": coalesce(col("lake_hash"), col("cache_hash")),
            }
        )
        .filter(col("status").isNotNull())
        .select(
            "id_chave_cliente",
            "num_cpfcnpj14",
            "cnpj9id",
            "num_cpfcnpj",
            "des_nome_cliente_razao_social",
            "des_cpfcnpj14_status",
            "hash",
            "status",
        )
    )
