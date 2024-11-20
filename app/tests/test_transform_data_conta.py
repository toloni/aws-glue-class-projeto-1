import pytest
from pyspark.sql import SparkSession, Row, DataFrame

from utils.sub_modules.transform import __transform_data
from utils.column_definitions import Base


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("unit-tests").getOrCreate()


def test_transform(spark):
    df_mesh = __mesh_data(spark)
    df_cache = __cache_data(spark)

    df_atual = __transform_data(df_mesh, df_cache, Base.CONTA)
    df_expected = __expected_data(spark)

    df_atual_row = {}
    df_expected_row = {}

    delta_columns = [
        "id",
        "des_segmentacao",
        "num_agencia",
        "numeroconta",
        "des_conta_status",
        "hash",
        "status",
    ]

    for conta in [74521, 74833, 74418]:
        # Filtrar e garantir que há apenas uma linha por CNPJ
        atual_rows = df_atual.filter(df_atual.numeroconta == conta).collect()
        expected_rows = df_expected.filter(df_expected.numeroconta == conta).collect()

        # Armazenar as linhas como dicionários para comparação
        df_atual_row[conta] = atual_rows[0].asDict()
        df_expected_row[conta] = expected_rows[0].asDict()

        # Comparar colunas, ignorando 'data_hora_processamento'
        for column in delta_columns:
            atual_value = df_atual_row[conta][column]
            expected_value = df_expected_row[conta][column]
            assert atual_value == expected_value, (
                f"Valores diferentes na coluna '{column}' para conta = {conta}: "
                f"esperado '{expected_value}', mas recebido '{atual_value}'"
            )


def __expected_data(spark) -> DataFrame:
    return spark.createDataFrame(
        [
            Row(
                id=None,
                des_segmentacao="CORPORATE",
                num_agencia="180",
                numeroconta="74521",
                des_conta_status="CONTA ATIVA",
                hash="8b8daf41a23efdc677dce66e31aa5adb5aa589d5e46b7dca39dee93b3178247a",
                status="I",
                data_hora_processamento="",
            ),
            Row(
                id="idcontadynamics181",
                des_segmentacao=None,
                num_agencia="181",
                numeroconta="74833",
                des_conta_status=None,
                hash="cfa859b220d61934a45483c06d7d6bc7ae093654a41a11e9258d09d3cab5663a",
                status="D",
                data_hora_processamento="",
            ),
            Row(
                id="idcontadynamics184",
                des_segmentacao="RETAIL",
                num_agencia="184",
                numeroconta="74418",
                des_conta_status="CONTA BLOQUEADA",
                hash="a4e701063a15e96725e04765726bf6c0be6f0938ffdd2da26c3a71fb8a6fce5f",
                status="U",
                data_hora_processamento="",
            ),
        ]
    )


def __mesh_data(spark) -> DataFrame:
    return spark.createDataFrame(
        [
            Row(
                des_segmentacao="CORPORATE",
                num_agencia="180",
                num_conta="7452",
                num_conta_dac="1",
                des_conta_status="CONTA ATIVA",
                cod_hierarquia_plataforma="742",
            ),
            Row(
                des_segmentacao="RETAIL",
                num_agencia="184",
                num_conta="7441",
                num_conta_dac="8",
                des_conta_status="CONTA BLOQUEADA",
                cod_hierarquia_plataforma="742",
            ),
        ]
    )


def __cache_data(spark) -> DataFrame:
    return spark.createDataFrame(
        [
            Row(
                id="idcontadynamics181",
                agencia="181",
                numeroconta="74833",
                hash="cfa859b220d61934a45483c06d7d6bc7ae093654a41a11e9258d09d3cab5663a",
                status="",
                datacriacao="",
            ),
            Row(
                id="idcontadynamics184",
                agencia="184",
                numeroconta="74418",
                hash="0000000000a4e701063a15e96725e04765726bf6c0be6f0938ffdd2da26c3a7",
                status="",
                datacriacao="",
            ),
        ]
    )
