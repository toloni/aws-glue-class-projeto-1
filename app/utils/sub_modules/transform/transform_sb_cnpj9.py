from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, concat, coalesce


# Transformação da base única - CNPJ9
def transform_cnpj9(
    df_encart_pj: DataFrame, df_cache: DataFrame, when_status
) -> DataFrame:
    """
    Realiza a transformação da base CNPJ9 unindo dados do encarte PJ e cache.

    Args:
        df_encart_pj (DataFrame): DataFrame contendo os dados do encarte PJ.
        df_cache (DataFrame): DataFrame contendo os dados do cache.
        when_status (function): Função que retorna as colunas de status com base em condições.

    Returns:
        DataFrame: DataFrame resultante após a transformação.
    """

    def prepare_encart_pj(df: DataFrame) -> DataFrame:
        """Prepara o DataFrame do encarte PJ filtrando e adicionando hash."""

        df = df.filter(col("num_cpfcnpj14").substr(-6, 6).contains("0001")).select(
            "num_cpfcnpj",
            "num_cpfcnpj14",
            "id_chave_cliente",
            "des_nome_cliente_razao_social",
            "des_cpfcnpj_status",
        )

        return (
            df.withColumn("l_key", col("num_cpfcnpj"))
            .dropDuplicates(["num_cpfcnpj"])
            .withColumn(
                "l_hash",
                sha2(
                    concat(
                        col("id_chave_cliente"),
                        col("des_nome_cliente_razao_social"),
                        col("des_cpfcnpj_status"),
                    ),
                    256,
                ),
            )
        )

    def prepare_cache(df: DataFrame) -> DataFrame:
        """Prepara o DataFrame de cache ajustando tipos e renomeando colunas."""
        df = df.withColumn("numerocnpj9", col("numerocnpj9").cast("int")).filter(
            col("numerocnpj9").isNotNull()
        )
        return (
            df.withColumn("r_key", col("numerocnpj9"))
            .withColumnRenamed("hash", "r_hash")
            .select(
                "id",
                "numerocnpj9",
                "nome",
                "cache_hash",
                "empresaprincipalid",
                "cache_hash",
                "r_key",
            )
        )

    # Preparo dos dados
    df_encart_pj = prepare_encart_pj(df_encart_pj)
    df_cache = prepare_cache(df_cache)
    df_cache.show()
    # União dos DataFrames
    df_joined = df_encart_pj.join(
        df_cache, df_encart_pj["l_key"] == df_cache["r_key"], "full"
    )

    # Transformação final
    return (
        df_joined.withColumns(when_status())
        .withColumns(
            {
                "num_cpfcnpj": coalesce(col("num_cpfcnpj"), col("numerocnpj9")),
                "des_nome_cliente_razao_social": coalesce(
                    col("des_nome_cliente_razao_social"), col("nome")
                ),
                "id_chave_cliente": coalesce(
                    col("id_chave_cliente"), col("empresaprincipalid")
                ),
                "hash": coalesce(col("lake_hash"), col("cache_hash")),
            }
        )
        .filter(col("status").isNotNull())
        .select(
            "id",
            "num_cpfcnpj",
            "des_nome_cliente_razao_social",
            "id_chave_cliente",
            "des_cpfcnpj_status",
            "hash",
            "status",
        )
    )
