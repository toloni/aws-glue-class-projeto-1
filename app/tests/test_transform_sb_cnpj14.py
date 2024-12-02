import pytest
from pyspark.sql import SparkSession, Row, DataFrame

from utils.sub_modules.transform.transform_sb_cnpj14 import transform_cnpj14
from utils.sub_modules.transform.transform import Transformer


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("unit-tests").getOrCreate()


def test_transform(spark):
    df_mesh = __mesh_data(spark)
    df_cache = __cache_data(spark)

    df_atual = transform_cnpj14(df_mesh, df_cache, Transformer().status_col)
    df_expected = __expected_data(spark)

    df_atual_row = {}
    df_expected_row = {}

    delta_columns = [
        "id_chave_cliente",
        "num_cpfcnpj14",
        "cnpj9id",
        "num_cpfcnpj",
        "des_nome_cliente_razao_social",
        "des_cpfcnpj14_status",
        "hash",
        "status",
    ]

    for cnpj in ["100000229", "10500092", "28000229"]:
        # Filtrar e garantir que há apenas uma linha por CNPJ
        atual_rows = df_atual.filter(df_atual.num_cpfcnpj14 == cnpj).collect()
        expected_rows = df_expected.filter(df_expected.num_cpfcnpj14 == cnpj).collect()

        # Armazenar as linhas como dicionários para comparação
        df_atual_row[cnpj] = atual_rows[0].asDict()
        df_expected_row[cnpj] = expected_rows[0].asDict()

        # Comparar colunas, ignorando 'data_hora_processamento'
        for column in delta_columns:
            atual_value = df_atual_row[cnpj][column]
            expected_value = df_expected_row[cnpj][column]
            assert atual_value == expected_value, (
                f"Valores diferentes na coluna '{column}' para num_cpfcnpj = {cnpj}: "
                f"esperado '{expected_value}', mas recebido '{atual_value}'"
            )


def __expected_data(spark) -> DataFrame:
    return spark.createDataFrame(
        [
            Row(
                id_chave_cliente="id14-100",
                num_cpfcnpj14="100000229",
                cnpj9id="id9-100",
                num_cpfcnpj="100",
                des_nome_cliente_razao_social=None,
                des_cpfcnpj14_status=None,
                hash="6a9061b1a48ef8ce7904cb89c10362389ea4c44b53d259da4b788fd78d401000",
                status="D",
                data_hora_processamento="",
            ),
            Row(
                id_chave_cliente="d1a4f2bd-6a8e-43b1-807e-91b5d2c3d8f3",
                num_cpfcnpj14="10500092",
                cnpj9id=None,
                num_cpfcnpj="105",
                des_nome_cliente_razao_social="EMPRESA 105",
                des_cpfcnpj14_status="CNPJ14 ATIVO",
                hash="a99d9528b5b2df1fbc67cc88e5853746badedd3e235b910913a4681da0cd0813",
                status="I",
                data_hora_processamento="",
            ),
            Row(
                id_chave_cliente="d69d3be2-0002-4c67-b013-fd826869ff84",
                num_cpfcnpj14="28000229",
                cnpj9id="id9-28",
                num_cpfcnpj="28",
                des_nome_cliente_razao_social="EMPRESA 28 EMPRESA 2",
                des_cpfcnpj14_status="CNPJ14 ATIVO",
                hash="1514f10b95d88d8372bb600ae8a91813f3c3bfa9d284c65b7e5fea3f25ee5462",
                status="U",
                data_hora_processamento="",
            ),
        ]
    )


def __mesh_data(spark) -> DataFrame:
    return spark.createDataFrame(
        [
            Row(
                num_cpfcnpj="105",
                num_cpfcnpj14="10500092",
                id_chave_cliente="d1a4f2bd-6a8e-43b1-807e-91b5d2c3d8f3",
                des_nome_cliente_razao_social="EMPRESA 105",
                des_cpfcnpj14_status="CNPJ14 ATIVO",
                cod_hierarquia_plataforma="742",
                cod_hierarquia_gq_segmento="A",
            ),
            Row(
                num_cpfcnpj="105",
                num_cpfcnpj14="10500092",
                id_chave_cliente="d1a4f2bd-6a8e-43b1-807e-91b5d2c3d8f3",
                des_nome_cliente_razao_social="EMPRESA 105",
                des_cpfcnpj14_status="CNPJ14 ATIVO",
                cod_hierarquia_plataforma="742",
                cod_hierarquia_gq_segmento="A",
            ),
            Row(
                num_cpfcnpj="105",
                num_cpfcnpj14="10500092",
                id_chave_cliente="d1a4f2bd-6a8e-43b1-807e-91b5d2c3d8f3",
                des_nome_cliente_razao_social="EMPRESA 105",
                des_cpfcnpj14_status="CNPJ14 ATIVO",
                cod_hierarquia_plataforma="742",
                cod_hierarquia_gq_segmento="A",
            ),
            Row(
                num_cpfcnpj="28",
                num_cpfcnpj14="28000229",
                id_chave_cliente="d69d3be2-0002-4c67-b013-fd826869ff84",
                des_nome_cliente_razao_social="EMPRESA 28 EMPRESA 2",
                des_cpfcnpj14_status="CNPJ14 ATIVO",
                cod_hierarquia_plataforma="742",
                cod_hierarquia_gq_segmento="A",
            ),
        ]
    )


def __cache_data(spark) -> DataFrame:
    return spark.createDataFrame(
        [
            Row(
                id="id14-28",
                cnpj="28000229",
                cnpj9="28",
                cnpj9id="id9-28",
                hash="6a9061b1a48ef8ce7904cb89c10362389ea4c44b53d259da4b788fd78d403820",
                status="",
                datacriacao="",
            ),
            Row(
                id="id14-100",
                cnpj="100000229",
                cnpj9="100",
                cnpj9id="id9-100",
                hash="6a9061b1a48ef8ce7904cb89c10362389ea4c44b53d259da4b788fd78d401000",
                status="",
                datacriacao="",
            ),
        ]
    )
