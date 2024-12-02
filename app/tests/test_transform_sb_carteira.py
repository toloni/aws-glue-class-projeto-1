import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from utils.sub_modules.transform.transform_sb_carteira import transform_carteira

# Função principal (transform_carteira) deve estar importada antes de executar os testes.


@pytest.fixture(scope="module")
def spark():
    """Cria uma SparkSession para os testes."""
    return (
        SparkSession.builder.master("local")
        .appName("TestTransformCarteira")
        .getOrCreate()
    )


def test_transform_carteira_insercao(spark):
    """Testa a inserção de registros no status 'I'."""
    # Dados de entrada
    df_encart_pj = spark.createDataFrame(
        [
            ("A", "B", "C", "D", "E", "", "", "", "", "ABCD"),
        ],
        [
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
        ],
    )
    df_cache = spark.createDataFrame(
        [
            (1, "A", "B", "C", "D", "E", "SD", "ABCD"),
        ],
        [
            "id",
            "segmentonegocio",
            "segmento",
            "plataforma",
            "numero",
            "regiao",
            "tipo",
            "cache_key",
        ],
    )

    # Executa a transformação
    result = transform_carteira(df_encart_pj, df_cache)

    # Valida o resultado
    expected_status = "I"
    assert result.filter(result.status == expected_status).count() == 1


def test_transform_carteira_exclusao(spark):
    """Testa a exclusão de registros no status 'D'."""
    # Dados de entrada
    df_encart_pj = spark.createDataFrame(
        [
            ("X", "X", "X", "X", "E", "", "", "", "", "ABCD"),
        ],
        [
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
        ],
    )
    df_cache = spark.createDataFrame(
        [
            (1, "A", "B", "004", "D", "E", "SD", "ABCD"),
        ],
        [
            "id",
            "segmentonegocio",
            "segmento",
            "plataforma",
            "numero",
            "regiao",
            "tipo",
            "cache_key",
        ],
    )

    # Executa a transformação
    result = transform_carteira(df_encart_pj, df_cache)

    # Valida o resultado
    expected_status = "D"
    assert result.filter(result.status == expected_status).count() == 1


def test_transform_carteira_tipo_carteira(spark):
    """Testa a atribuição correta do tipo de carteira."""
    # Dados de entrada
    df_encart_pj = spark.createDataFrame(
        [
            ("A", "B", "C", "D", "E", "", "", "", "", "ABCD"),
        ],
        [
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
        ],
    )
    df_cache = spark.createDataFrame(
        [
            (1, "A", "B", "004", "D", "E", "", "ABCD"),
        ],
        [
            "id",
            "segmentonegocio",
            "segmento",
            "plataforma",
            "numero",
            "regiao",
            "tipo",
            "cache_key",
        ],
    )

    # Executa a transformação
    result = transform_carteira(df_encart_pj, df_cache)

    # Valida o tipo de carteira
    assert result.filter(result.tipo == "CO").count() == 1


def test_transform_carteira_uniao(spark):
    """Testa a união dos DataFrames."""
    # Dados de entrada
    df_encart_pj = spark.createDataFrame(
        [
            ("A", "B", "C", "D", "E", "", "", "", "", "ABCD"),
        ],
        [
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
        ],
    )
    df_cache = spark.createDataFrame(
        [
            (1, "X", "Y", "0", "W", "E", "", "WXYZ"),
        ],
        [
            "id",
            "segmentonegocio",
            "segmento",
            "plataforma",
            "numero",
            "regiao",
            "tipo",
            "cache_key",
        ],
    )

    # Executa a transformação
    result = transform_carteira(df_encart_pj, df_cache)

    # Valida se ambos os registros estão no resultado
    assert result.count() == 2
