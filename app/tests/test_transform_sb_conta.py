import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import lit
from utils.sub_modules.transform.transform_sb_conta import transform_conta
from utils.sub_modules.transform.transform import Transformer


@pytest.fixture(scope="module")
def spark():
    """Configura a sessão PySpark para os testes."""
    return (
        SparkSession.builder.master("local[*]")
        .appName("TestTransformConta")
        .getOrCreate()
    )


@pytest.fixture
def df_encart_pj(spark):
    """Cria um DataFrame de encarte PJ para os testes."""
    data = [
        ("1", "seg1", "A", "101", "202", "9", "C1", "2022-01-01", "G1"),
        ("2", "seg2", "B", "102", "203", "7", "C2", "2022-02-01", "G2"),
    ]
    schema = StructType(
        [
            StructField("cod_hierarquia_gq_segmento", StringType(), True),
            StructField("des_segmentacao", StringType(), True),
            StructField("id_chave_conta", StringType(), True),
            StructField("num_agencia", StringType(), True),
            StructField("num_conta", StringType(), True),
            StructField("num_conta_dac", StringType(), True),
            StructField("id_chave_cliente", StringType(), True),
            StructField("dat_inicio_relacionamento_harmonizado", StringType(), True),
            StructField("cod_conta_gestora", StringType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)


@pytest.fixture
def df_cache(spark):
    """Cria um DataFrame de cache para os testes."""
    data = [
        ("A", 101, 2029, "hash1"),
        (
            "B",
            102,
            2037,
            "1aac123d660ea3e4b4a7ed7298d42ef723b2adde919f99f97f1dbb1145135f55",
        ),
        (
            "C",
            103,
            2045,
            "1aac123d660ea3e4b4a7ed7298d42ef723b2adde919f99f97f1dbb1145135f55",
        ),
    ]
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("agencia", IntegerType(), True),
            StructField("numeroconta", IntegerType(), True),
            StructField("hash", StringType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)


def test_transform_conta_inclusao(spark, df_encart_pj, df_cache):
    """Testa inclusão de dados (`status = 'I'`)."""
    df_cache_empty = df_cache.filter(lit(False))  # Cache vazio
    result = transform_conta(df_encart_pj, df_cache_empty, Transformer().status_col)
    result_data = result.filter(result["status"] == "I").collect()
    assert len(result_data) == df_encart_pj.count()


def test_transform_conta_exclusao(spark, df_encart_pj, df_cache):
    """Testa exclusão de dados (`status = 'D'`)."""
    df_encart_pj_empty = df_encart_pj.filter(lit(False))  # Encarte PJ vazio
    result = transform_conta(df_encart_pj_empty, df_cache, Transformer().status_col)
    result_data = result.filter(result["status"] == "D").collect()
    assert len(result_data) == df_cache.count()


def test_transform_conta_atualizacao(spark, df_encart_pj, df_cache):
    """Testa atualização de dados (`status = 'U'`)."""
    result = transform_conta(df_encart_pj, df_cache, Transformer().status_col)
    result_data = result.filter(result["status"] == "U").collect()
    assert len(result_data) == 1  # Apenas uma conta foi atualizada


def test_transform_conta_sem_mudancas(spark, df_encart_pj, df_cache):
    """Testa que contas iguais não possuem `status`."""
    # Adiciona o mesmo hash para simular ausência de mudanças

    df_encart_pj = df_encart_pj.withColumns(
        {
            "cod_hierarquia_gq_segmento": lit("1"),
            "des_segmentacao": lit("seg1"),
            "id_chave_cliente": lit("A"),
            "cod_conta_gestora": lit("101"),
            "num_agencia": lit("1"),
            "num_conta": lit("20"),
            "num_conta_dac": lit("2"),
        }
    )
    df_cache = df_cache.withColumns(
        {
            "hash": lit(
                "c997840f4dbeea4c46f8059521f3eea02716b2e75c8fc632288d998ce94e902c"
            ),
            "agencia": lit("1"),
            "numeroconta": lit("202"),
        },
    )
    result = transform_conta(df_encart_pj, df_cache, Transformer().status_col)
    result_data = result.filter(result["status"].isNotNull()).collect()
    assert len(result_data) == 0
