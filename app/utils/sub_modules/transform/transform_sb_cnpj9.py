from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, concat, coalesce


# Transform Single Base - CNPJ9
# ------------------------------------------------------------------------- #
def transform_cnpj9(
    df_encart_pj: DataFrame, df_cache: DataFrame, when_status
) -> DataFrame:

    # remover filiais
    df_encart_pj = df_encart_pj.filter(
        col("num_cpfcnpj14").substr(-6, 6).contains("0001")
    ).select(
        "num_cpfcnpj",
        "num_cpfcnpj14",
        "id_chave_cliente",
        "des_nome_cliente_razao_social",
        "des_cpfcnpj_status",
    )

    # adicionar hash eliminar duplicados
    df_encart_pj = df_encart_pj.dropDuplicates(["num_cpfcnpj"]).withColumn(
        "lake_hash",
        sha2(
            concat(
                "id_chave_cliente",
                "des_nome_cliente_razao_social",
                "des_cpfcnpj_status",
            ),
            256,
        ),
    )

    # preparar cache
    df_cache = df_cache.withColumn(
        "numerocnpj9", df_cache["numerocnpj9"].cast("int")
    ).withColumnRenamed("hash", "cache_hash")

    df_joined = df_encart_pj.join(
        df_cache, df_encart_pj["num_cpfcnpj"] == df_cache["numerocnpj9"], "full"
    )

    # realizar join e aplicar transformações
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
