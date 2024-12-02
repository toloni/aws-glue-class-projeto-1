from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, concat, coalesce, when, isnull, lit


# Transformação da base única - Carteira
def transform_carteira(df_encart_pj: DataFrame, df_cache: DataFrame) -> DataFrame:
    """
    Realiza a transformação da base Carteira unindo dados do encarte PJ e cache.

    Args:
        df_encart_pj (DataFrame): DataFrame contendo os dados do encarte PJ.
        df_cache (DataFrame): DataFrame contendo os dados do cache.
        when_status (function): Função que retorna as colunas de status com base em condições.

    Returns:
        DataFrame: DataFrame resultante após a transformação.
    """

    def prepare_encart_pj(df: DataFrame) -> DataFrame:
        """Prepara o DataFrame do encarte PJ filtrando e adicionando hash e chave."""
        chave_carteira = [
            "des_segmentacao",
            "cod_hierarquia_gq_segmento",
            "cod_hierarquia_plataforma",
            "cod_hierarquia_gerente",
        ]

        df = (
            df.dropDuplicates(chave_carteira)
            .withColumn("lake_key", concat(*chave_carteira))
            .select(
                "des_segmentacao",
                "cod_hierarquia_gq_segmento",
                "cod_hierarquia_plataforma",
                "cod_hierarquia_gerente",
                "cod_hierarquia_regiao",
                "cod_celu_digl_ated",
                "cod_crtr_asst_celu_digl",
                "cod_plat_invt_clie",
                "cod_crtr_epct_invt",
                "lake_key",
            )
        )

        # CO - !isNull(PLAT) && !isNull(GER) && PLAT != '' && GER ! =  ''
        # SD - !isNul(PLATAFCD) && !isNull(CODGERCD) && PLATAFC ! = '' && CODGERCD ! = ''
        # PA - !isNull(PLATAFCS) && !isNull(CODGERCS) && PLATAFCS ! = '' && CODGERCS != ''

        # PLAT - cod_hierarquia_plataforma
        # GER - cod_hierarquia_gerente
        # ----
        # PLATAFCD - cod_celu_digl_ated
        # CODGERCD - cod_crtr_asst_celu_digl
        # ----
        # PLATAFCS - cod_plat_invt_clie
        # CODGERCS - cod_crtr_epct_invt

        # Tipo Carteira
        df = df.withColumn(
            "tipo_carteira",
            when(
                (~isnull(col("cod_hierarquia_plataforma")))
                & (~isnull(col("cod_hierarquia_gerente")))
                & (col("cod_hierarquia_plataforma") != "")
                & (col("cod_hierarquia_gerente") != ""),
                "CO",
            )
            .when(
                (~isnull(col("cod_celu_digl_ated")))
                & (~isnull(col("cod_crtr_asst_celu_digl")))
                & (col("cod_celu_digl_ated") != "")
                & (col("cod_crtr_asst_celu_digl") != ""),
                "SD",
            )
            .when(
                (~isnull(col("cod_plat_invt_clie")))
                & (~isnull(col("cod_crtr_epct_invt")))
                & (col("cod_plat_invt_clie") != "")
                & (col("cod_crtr_epct_invt") != ""),
                "PA",
            )
            .otherwise(lit("")),  # Valor padrão, se nenhuma condição for atendida
        )

        return df

    def prepare_cache(df: DataFrame) -> DataFrame:
        """Prepara o DataFrame de cache ajustando nomes e criando chave."""
        chave_carteira = [
            "segmentonegocio",
            "segmento",
            "plataforma",
            "numero",
        ]

        df = df.withColumn("plataforma", col("plataforma").cast("int")).select(
            "id",
            "segmentonegocio",
            "segmento",
            "plataforma",
            "numero",
            "regiao",
            "tipo",
        )

        return df.withColumn(
            "cache_key",
            concat(*chave_carteira),
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
        df_joined.withColumn(
            "status",
            when(
                col("lake_key").isNotNull() & col("cache_key").isNull(),
                lit("I"),
            ).when(
                col("lake_key").isNull() & col("cache_key").isNotNull(),
                lit("D"),
            ),
        )
        .withColumns(
            {
                "segmentonegocio": coalesce(
                    col("des_segmentacao"), col("segmentonegocio")
                ),
                "segmento": coalesce(
                    col("cod_hierarquia_gq_segmento"), col("segmento")
                ),
                "plataforma": coalesce(
                    col("cod_hierarquia_plataforma"), col("plataforma")
                ),
                "numero": coalesce(col("cod_hierarquia_gerente"), col("numero")),
                "regiao": coalesce(col("cod_hierarquia_regiao"), col("regiao")),
                "tipo": coalesce(col("tipo_carteira"), col("tipo")),
            }
        )
        .filter(col("status").isNotNull())
        .select(
            "id",
            "segmentonegocio",
            "segmento",
            "plataforma",
            "numero",
            "regiao",
            "tipo",
            "status",
        )
    )
