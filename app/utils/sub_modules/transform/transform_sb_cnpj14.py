from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, concat, coalesce


# Transform Single Base - CNPJ14
# ------------------------------------------------------------------------- #
def transform_cnpj14(
    df_encart_pj: DataFrame, df_cache: DataFrame, when_status
) -> DataFrame:

    # remover filiais
    df_encart_pj = df_encart_pj.select(
        "num_cpfcnpj",
        "num_cpfcnpj14",
        "id_chave_cliente",
        "des_nome_cliente_razao_social",
        "des_cpfcnpj14_status",
    )

    # adicionar hash eliminar duplicados
    df_encart_pj = df_encart_pj.dropDuplicates(["num_cpfcnpj14"]).withColumn(
        "lake_hash",
        sha2(
            concat(
                "id_chave_cliente",
                "des_nome_cliente_razao_social",
                "des_cpfcnpj14_status",
            ),
            256,
        ),
    )

    # preparar cache
    df_cache = df_cache.withColumnRenamed("hash", "cache_hash")

    df_joined = df_encart_pj.join(
        df_cache, df_encart_pj["num_cpfcnpj14"] == df_cache["cnpj"], "full"
    )

    # realizar join e aplicar transformações
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
