import pytest
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from utils.transform import transform


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("unit-tests").getOrCreate()


def read_csv_df(spark, date):
    return spark.read.csv(f"app/tests/cliente_{date}.csv", header="True")


# Mock de DateToProcess para testar datas
class MockDateToProcess:
    def get_last_date(self, base: str):
        return 20241030

    def get_prev_date(self, base: str):
        return 20241020


class MockDateToProcessFull:
    def get_last_date(self, base: str):
        return 20241030

    def get_prev_date(self, base: str):
        return None


# ---------------------------------------------------------------------------- #
# Testes para a função transform para CNPJ9
def test_transform_cnpj9(spark):
    partitions_mesh_df = {
        20241030: read_csv_df(spark, 20241030),
        20241020: read_csv_df(spark, 20241020),
    }
    partitions_dates = MockDateToProcess()
    base = "CNPJ9"
    result_df = transform(partitions_mesh_df, partitions_dates, base)
    result_data = result_df.collect()

    assert len(result_data) == 7

    # Verificar Status dos CNPJs
    expected_status = {
        "50": "I",
        "60": "I",
        "75": "I",
        "105": "D",
        "92": "D",
        "28": "U",
        "45": "U",
    }
    assert all(
        row.status == expected_status.get(row.num_cpfcnpj, row.status)
        for row in result_data
    ), "Alguns CNPJs não possuem o status esperado."

    # Verificar se existe campos vazios ou nulos
    assert all(
        all(cell is not None and cell != "" for cell in row) for row in result_data
    ), "Existem campos vazios na tabela."

    # Lista das colunas esperadas
    expected_columns = {
        "num_cpfcnpj",
        "id_chave_cliente",
        "des_nome_cliente_razao_social",
        "des_cpfcnpj_status",
        "anomesdia",
        "status",
        "data_processamento",
    }

    # Verificação de colunas no DataFrame
    assert (
        set(result_data[0].__fields__) == expected_columns
    ), "Nem todas as colunas esperadas estão presentes na tabela."

    # Verificar numero de Colunas
    for row in result_data:
        assert (
            len(row) == 7
        ), f"A linha com num_cpfcnpj {row.num_cpfcnpj} tem um número incorreto de colunas."


# ---------------------------------------------------------------------------- #
# Testes para a função transform para CNPJ9
def test_transform_cnpj9_full(spark):
    partitions_mesh_df = {
        20241030: read_csv_df(spark, 20241030),
    }
    partitions_dates = MockDateToProcessFull()
    base = "CNPJ9"
    result_df = transform(partitions_mesh_df, partitions_dates, base)
    result_data = result_df.collect()

    # ASSERTS

    assert len(result_data) == 8

    # Verificar Status dos CNPJs
    expected_status = {
        "50": "FULL",
        "60": "FULL",
        "75": "FULL",
        "105": "FULL",
        "92": "FULL",
        "28": "FULL",
        "45": "FULL",
    }
    assert all(
        row.status == expected_status.get(row.num_cpfcnpj, row.status)
        for row in result_data
    ), "Alguns CNPJs não possuem o status esperado."

    # Verificar se existe campos vazios ou nulos
    assert all(
        all(cell is not None and cell != "" for cell in row) for row in result_data
    ), "Existem campos vazios na tabela."

    # Lista das colunas esperadas
    expected_columns = {
        "num_cpfcnpj",
        "id_chave_cliente",
        "des_nome_cliente_razao_social",
        "des_cpfcnpj_status",
        "anomesdia",
        "status",
        "data_processamento",
    }

    # Verificação de colunas no DataFrame
    assert (
        set(result_data[0].__fields__) == expected_columns
    ), "Nem todas as colunas esperadas estão presentes na tabela."

    # Verificar numero de Colunas
    for row in result_data:
        assert (
            len(row) == 7
        ), f"A linha com num_cpfcnpj {row.num_cpfcnpj} tem um número incorreto de colunas."


# ---------------------------------------------------------------------------- #
# Testes para a função transform para CNPJ14
def test_transform_cnpj14(spark):
    partitions_mesh_df = {
        20241030: read_csv_df(spark, 20241030),
        20241020: read_csv_df(spark, 20241020),
    }
    partitions_dates = MockDateToProcess()
    base = "CNPJ14"
    result_df = transform(partitions_mesh_df, partitions_dates, base)
    result_data = result_df.collect()

    assert len(result_data) == 11

    # Verificar Status dos CNPJs
    expected_status = {
        "28000229": "I",
        "34000291": "I",
        "34000391": "I",
        "34000491": "I",
        "50000418": "I",
        "60000572": "I",
        "75000689": "I",
        "10500092": "D",
        "92000813": "D",
        "28000129": "U",
        "45000354": "U",
    }
    assert all(
        row.status == expected_status.get(row.num_cpfcnpj14, row.status)
        for row in result_data
    ), "Alguns CNPJs não possuem o status esperado."

    # Verificar se existe campos vazios ou nulos
    assert all(
        all(cell is not None and cell != "" for cell in row) for row in result_data
    ), "Existem campos vazios na tabela."

    # Lista das colunas esperadas
    expected_columns = {
        "num_cpfcnpj14",
        "num_cpfcnpj",
        "id_chave_cliente",
        "des_nome_cliente_razao_social",
        "des_cpfcnpj14_status",
        "anomesdia",
        "status",
        "data_processamento",
    }

    # Verificação de colunas no DataFrame
    assert (
        set(result_data[0].__fields__) == expected_columns
    ), "Nem todas as colunas esperadas estão presentes na tabela."

    # Verificar numero de Colunas
    for row in result_data:
        assert (
            len(row) == 8
        ), f"A linha com num_cpfcnpj14 {row.num_cpfcnpj14} tem um número incorreto de colunas."


# ---------------------------------------------------------------------------- #
# Testes para a função transform para CONTA
def test_transform_conta(spark):
    partitions_mesh_df = {
        20241030: read_csv_df(spark, 20241030),
        20241020: read_csv_df(spark, 20241020),
    }
    partitions_dates = MockDateToProcess()
    base = "CONTA"
    result_df = transform(partitions_mesh_df, partitions_dates, base)
    result_data = result_df.collect()

    assert len(result_data) == 7

    # Verificar Status das contas
    expected_status = {
        "7427": "I",
        "7441": "I",
        "7439": "I",
        "7466": "D",
        "7478": "D",
        "7483": "U",
        "7399": "U",
    }
    assert all(
        row.status == expected_status.get(row.num_conta, row.status)
        for row in result_data
    ), "Alguns CNPJs não possuem o status esperado."

    # Verificar se existe campos vazios ou nulos
    assert all(
        all(cell is not None and cell != "" for cell in row) for row in result_data
    ), "Existem campos vazios na tabela."

    # Lista das colunas esperadas
    expected_columns = {
        "num_agencia",
        "num_conta",
        "num_conta_dac",
        "des_segmentacao",
        "des_conta_status",
        "anomesdia",
        "status",
        "data_processamento",
    }

    # Verificação de colunas no DataFrame
    assert (
        set(result_data[0].__fields__) == expected_columns
    ), "Nem todas as colunas esperadas estão presentes na tabela."

    # Verificar numero de Colunas
    for row in result_data:
        assert (
            len(row) == 8
        ), f"A linha com num_conta {row.num_conta} tem um número incorreto de colunas."


# ---------------------------------------------------------------------------- #
# Testes para a função transform para CARTEIRA
def test_transform_carteira(spark):
    partitions_mesh_df = {
        20241030: read_csv_df(spark, 20241030),
        20241020: read_csv_df(spark, 20241020),
    }
    partitions_dates = MockDateToProcess()
    base = "CARTEIRA"
    result_df = transform(partitions_mesh_df, partitions_dates, base)
    result_data = result_df.collect()

    assert len(result_data) == 7

    # Verificar Status das contas
    expected_status = {
        "742": "I",
        "744": "I",
        "743": "I",
        "750": "I",
        "746": "D",
        "738": "D",
        "738": "D",
    }
    assert all(
        row.status == expected_status.get(row.cod_hierarquia_plataforma, row.status)
        for row in result_data
    ), "Algumas CARTEIRAS não possuem o status esperado."

    # Verificar se existe campos vazios ou nulos
    assert all(
        all(cell is not None and cell != "" for cell in row) for row in result_data
    ), "Existem campos vazios na tabela."

    # Lista das colunas esperadas
    expected_columns = {
        "cod_hierarquia_gq_segmento",
        "cod_hierarquia_plataforma",
        "cod_hierarquia_gerente",
        "anomesdia",
        "status",
        "data_processamento",
    }

    # Verificação de colunas no DataFrame
    assert (
        set(result_data[0].__fields__) == expected_columns
    ), "Nem todas as colunas esperadas estão presentes na tabela."

    # Verificar numero de Colunas
    for row in result_data:
        assert (
            len(row) == 6
        ), f"A linha com cod_hierarquia_plataforma {row.cod_hierarquia_plataforma} tem um número incorreto de colunas."
