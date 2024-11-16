import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from utils.sub_modules.transform import transform_load
from utils.enum import Base
from tests.test_transform_data_cnpj9 import __cache_data as cache_cnpj9
from tests.test_transform_data_cnpj14 import __cache_data as cache_cnpj14
from tests.test_transform_data_carteira import __cache_data as cache_carteira
from tests.test_transform_data_conta import __cache_data as cache_conta

# Esquema do DataFrame
schema = StructType(
    [
        StructField("id_chave_cliente", StringType(), True),
        StructField("num_cpfcnpj", IntegerType(), True),
        StructField("num_cpfcnpj14", IntegerType(), True),
        StructField("des_nome_cliente_razao_social", StringType(), True),
        StructField("cod_hierarquia_gq_segmento", StringType(), True),
        StructField("des_segmentacao", StringType(), True),
        StructField("cod_hierarquia_plataforma", IntegerType(), True),
        StructField("cod_hierarquia_gerente", IntegerType(), True),
        StructField("cod_hierarquia_regiao", IntegerType(), True),
        StructField("num_agencia", IntegerType(), True),
        StructField("num_conta", IntegerType(), True),
        StructField("num_conta_dac", IntegerType(), True),
        StructField("des_cpfcnpj_status", StringType(), True),
        StructField("des_cpfcnpj14_status", StringType(), True),
        StructField("des_conta_status", StringType(), True),
        StructField("cliente_desde", StringType(), True),
        StructField("anomesdia", StringType(), True),
        StructField("filial", StringType(), True),
    ]
)

# Dados
data = [
    (
        "d69d3be2-0001-4c67-b013-fd826869ff84",
        28,
        28000129,
        "EMPRESA 28 NOVO NOME",
        "D",
        "BUSINESS",
        750,
        51,
        12,
        186,
        7399,
        5,
        "CNPJ9 ATIVO",
        "CNPJ14 ATIVO",
        "CONTA ENCERRADA",
        "19940701",
        "20241030",
        "0001",
    )
]


@pytest.fixture(scope="module")
def spark():
    """Cria uma sessão Spark para os testes."""
    spark_session = (
        SparkSession.builder.master("local").appName("unit-tests").getOrCreate()
    )
    yield spark_session
    spark_session.stop()


@pytest.fixture
def df_input(spark):
    """Cria o DataFrame de entrada para os testes."""
    return spark.createDataFrame(data, schema=schema)


@pytest.fixture
def dict_df_cache(spark):
    """Cria um dicionário com os DataFrames de cache simulados."""
    return {
        Base.CNPJ9: cache_cnpj9(spark),
        Base.CNPJ14: cache_cnpj14(spark),
        Base.CARTEIRA: cache_carteira(spark),
        Base.CONTA: cache_conta(spark),
    }


@pytest.fixture
def args():
    """Argumentos simulados para a função."""
    return {
        "OUTPUT_S3_PATH_DELTA_CNPJ9": "s3://fake-bucket/delta/cnpj9",
        "OUTPUT_S3_PATH_DELTA_CNPJ14": "s3://fake-bucket/delta/cnpj14",
        "OUTPUT_S3_PATH_DELTA_CARTEIRA": "s3://fake-bucket/delta/carteira",
        "OUTPUT_S3_PATH_DELTA_CONTA": "s3://fake-bucket/delta/conta",
    }


@patch("utils.sub_modules.transform.load")  # Mock da função load
@patch("utils.sub_modules.transform.logging.getLogger")  # Mock do logger
def test_transform_load(
    mock_get_logger, mock_load, spark, df_input, dict_df_cache, args
):
    # Mock do logger
    mock_logger = MagicMock()
    mock_get_logger.return_value = mock_logger

    # Chamar a função com os dados simulados
    transform_load(df_input, dict_df_cache, args)

    # Verificar se a função load foi chamada corretamente
    mock_load.assert_called()  # Verifica que foi chamada ao menos uma vez
    assert mock_load.call_count == len(Base)  # Uma chamada para cada base
