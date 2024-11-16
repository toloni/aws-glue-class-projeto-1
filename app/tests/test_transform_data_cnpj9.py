import pytest
from pyspark.sql import SparkSession, Row, DataFrame

from utils.sub_modules.transform import __transform_data


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("unit-tests").getOrCreate()


def test_transform(spark):
    df_mesh = __mesh_data(spark)
    df_cache = __cache_data(spark)

    base = "CNPJ9"
    df_atual = __transform_data(df_mesh, df_cache, base)
    df_expected = __expected_data(spark)

    df_atual_row = {}
    df_expected_row = {}

    cnpj9_delta_columns = [
        "id",
        "num_cpfcnpj",
        "des_nome_cliente_razao_social",
        "id_chave_cliente",
        "des_cpfcnpj_status",
        "hash",
        "status",
    ]

    for cnpj in [1, 4, 5]:
        # Filtrar e garantir que há apenas uma linha por CNPJ
        atual_rows = df_atual.filter(df_atual.num_cpfcnpj == cnpj).collect()
        expected_rows = df_expected.filter(df_expected.num_cpfcnpj == cnpj).collect()

        # Armazenar as linhas como dicionários para comparação
        df_atual_row[cnpj] = atual_rows[0].asDict()
        df_expected_row[cnpj] = expected_rows[0].asDict()

        # Comparar colunas, ignorando 'data_hora_processamento'
        for column in cnpj9_delta_columns:
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
                id="IDDYN000-0000-0000-0000-000000000001",
                num_cpfcnpj="1",
                des_nome_cliente_razao_social="Sociedade Esportiva Palmeiras - ATUALIZACAO NOME",
                id_chave_cliente="00000000-0000-0000-0000-000000000001",
                des_cpfcnpj_status="CAMPEAO",
                hash="8d434b807a9e8f68afbdc5b8ac59abbcd1f0681b501f68d78774e6fcac830996",
                status="U",
                data_hora_processamento="",
            ),
            Row(
                id=None,
                num_cpfcnpj="4",
                des_nome_cliente_razao_social="São Paulo Futebol Clube - INSERIDO",
                id_chave_cliente="00000000-0000-0000-0000-000000000004",
                des_cpfcnpj_status="NOVO",
                hash="df07a8ccbcf26f46854da048e26e6162da566ed3140cf933e8fe49d660399d29",
                status="I",
                data_hora_processamento="",
            ),
            Row(
                id="IDDYN000-0000-0000-0000-000000000005",
                num_cpfcnpj="5",
                des_nome_cliente_razao_social="Sport Club Corinthians Paulista - EXCLUIDO",
                id_chave_cliente="00000000-0000-0000-0000-000000000005",
                des_cpfcnpj_status=None,
                hash="ba08094e1d09cae88d184d9069a925f9571915566e55c2b410849f9670dec94a",
                status="D",
                data_hora_processamento="",
            ),
        ]
    )


def __mesh_data(spark) -> DataFrame:
    return spark.createDataFrame(
        [
            Row(
                num_cpfcnpj="1",
                num_cpfcnpj14="1000101",
                id_chave_cliente="00000000-0000-0000-0000-000000000001",
                des_nome_cliente_razao_social="Sociedade Esportiva Palmeiras - ATUALIZACAO NOME",
                des_cpfcnpj_status="CAMPEAO",
            ),
            Row(
                num_cpfcnpj="1",
                num_cpfcnpj14="1000201",
                id_chave_cliente="00000000-0000-0000-0002-000000000001",
                des_nome_cliente_razao_social="Sociedade Esportiva Palmeiras - Sub 20 Juniores",
                des_cpfcnpj_status="CAMPEAO",
            ),
            Row(
                num_cpfcnpj="2",
                num_cpfcnpj14="2000102",
                id_chave_cliente="00000000-0000-0000-0000-000000000002",
                des_nome_cliente_razao_social="Clube de Regatas do Flamengo - SEM ALTERACAO",
                des_cpfcnpj_status="CLASSIFICADO",
            ),
            Row(
                num_cpfcnpj="4",
                num_cpfcnpj14="4000104",
                id_chave_cliente="00000000-0000-0000-0000-000000000004",
                des_nome_cliente_razao_social="São Paulo Futebol Clube - INSERIDO",
                des_cpfcnpj_status="NOVO",
            ),
        ]
    )


def __cache_data(spark) -> DataFrame:
    return spark.createDataFrame(
        [
            Row(
                id="IDDYN000-0000-0000-0000-000000000001",
                numerocnpj9="1",
                nome="Sociedade Esportiva Palmeiras",
                empresaprincipalid="00000000-0000-0000-0000-000000000001",
                hash="ba08094e1d09cae88d184d9069a925f9571915566e55c2b410849f9670dec94a",
                status="",
                datacriacao="2024-11-15",
            ),
            Row(
                id="IDDYN000-0000-0000-0000-000000000002",
                numerocnpj9="2",
                nome="Clube de Regatas do Flamengo - SEM ALTERACAO",
                empresaprincipalid="00000000-0000-0000-0000-000000000002",
                hash="cfae9e9845cefe10a1ac3221a1cf9e6dd77d6415fe82d83a32380273f9e984e9",
                status="",
                datacriacao="2024-11-15",
            ),
            Row(
                id="IDDYN000-0000-0000-0000-000000000005",
                numerocnpj9="5",
                nome="Sport Club Corinthians Paulista - EXCLUIDO",
                empresaprincipalid="00000000-0000-0000-0000-000000000005",
                hash="ba08094e1d09cae88d184d9069a925f9571915566e55c2b410849f9670dec94a",
                status="",
                datacriacao="2024-11-15",
            ),
        ]
    )
