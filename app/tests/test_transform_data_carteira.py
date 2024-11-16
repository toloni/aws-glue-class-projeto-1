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

    df_atual = __transform_data(df_mesh, df_cache, Base.CARTEIRA)
    df_expected = __expected_data(spark)

    df_atual_row = {}
    df_expected_row = {}

    delta_columns = [
        "id",
        "des_segmentacao",
        "cod_hierarquia_gq_segmento",
        "cod_hierarquia_plataforma",
        "cod_hierarquia_gerente",
        "cod_hierarquia_regiao",
        "hash",
        "status",
    ]

    for plataforma in [742, 746]:
        # Filtrar e garantir que há apenas uma linha por CNPJ
        atual_rows = df_atual.filter(
            df_atual.cod_hierarquia_plataforma == plataforma
        ).collect()
        expected_rows = df_expected.filter(
            df_expected.cod_hierarquia_plataforma == plataforma
        ).collect()

        # Armazenar as linhas como dicionários para comparação
        df_atual_row[plataforma] = atual_rows[0].asDict()
        df_expected_row[plataforma] = expected_rows[0].asDict()

        # Comparar colunas, ignorando 'data_hora_processamento'
        for column in delta_columns:
            atual_value = df_atual_row[plataforma][column]
            expected_value = df_expected_row[plataforma][column]
            assert atual_value == expected_value, (
                f"Valores diferentes na coluna '{column}' para plataforma = {plataforma}: "
                f"esperado '{expected_value}', mas recebido '{atual_value}'"
            )


def __expected_data(spark) -> DataFrame:
    return spark.createDataFrame(
        [
            Row(
                id="id74260",
                des_segmentacao=None,
                cod_hierarquia_gq_segmento="A",
                cod_hierarquia_plataforma="742",
                cod_hierarquia_gerente="50",
                cod_hierarquia_regiao=None,
                hash="3a4a11653f39c021cb782c32c4ff68352bc8889ad2db1e4ac8d79b1d1fc86d9b",
                status="D",
                data_hora_processamento="",
            ),
            Row(
                id=None,
                des_segmentacao="PRIVATE",
                cod_hierarquia_gq_segmento="A",
                cod_hierarquia_plataforma="746",
                cod_hierarquia_gerente="68",
                cod_hierarquia_regiao="40",
                hash="4c12c939628e8a027e668c493250994b7e53dd2b7390b4306975195aa1dd4e83",
                status="I",
                data_hora_processamento="",
            ),
        ]
    )


def __mesh_data(spark) -> DataFrame:
    return spark.createDataFrame(
        [
            Row(
                cod_hierarquia_gq_segmento="A",
                cod_hierarquia_plataforma="742",
                cod_hierarquia_gerente="58",
                cod_hierarquia_regiao="23",
                des_segmentacao="PRIVATE",
            ),
            Row(
                cod_hierarquia_gq_segmento="A",
                cod_hierarquia_plataforma="742",
                cod_hierarquia_gerente="58",
                cod_hierarquia_regiao="23",
                des_segmentacao="PRIVATE",
            ),
            Row(
                cod_hierarquia_gq_segmento="A",
                cod_hierarquia_plataforma="746",
                cod_hierarquia_gerente="68",
                cod_hierarquia_regiao="40",
                des_segmentacao="PRIVATE",
            ),
        ]
    )


def __cache_data(spark) -> DataFrame:
    return spark.createDataFrame(
        [
            Row(
                id="id742",
                numero="58",
                plataforma="742",
                segmento="A",
                tipo="CO",
                hash="3a4a11653f39c021cb782c32c4ff68352bc8889ad2db1e4ac8d79b1d1fc86d9b",
                status="",
                datacriacao="",
            ),
            Row(
                id="id74260",
                numero="50",
                plataforma="742",
                segmento="A",
                tipo="CO",
                hash="3a4a11653f39c021cb782c32c4ff68352bc8889ad2db1e4ac8d79b1d1fc86d9b",
                status="",
                datacriacao="",
            ),
        ]
    )
