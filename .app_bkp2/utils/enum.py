from enum import Enum


class Base(Enum):
    """
    Nome das Bases para Processamento
    """

    CNPJ9 = "CNPJ9"
    CNPJ14 = "CNPJ14"
    CONTA = "CONTA"
    CARTEIRA = "CARTEIRA"


class ColsPK(Enum):
    """
    Colunas Chaves para utilização no Join
    """

    CNPJ9 = ["num_cpfcnpj"]
    CNPJ14 = ["num_cpfcnpj14"]
    CONTA = [
        "num_agencia",
        "num_conta",
        "num_conta_dac",
    ]
    CARTEIRA = [
        "cod_hierarquia_gq_segmento",
        "cod_hierarquia_plataforma",
        "cod_hierarquia_gerente",
    ]


class ColsHASH(Enum):
    """
    Colunas para utilização no Join
    """

    CNPJ9 = [
        "id_chave_cliente",
        "des_nome_cliente_razao_social",
        "des_cpfcnpj_status",
    ]
    CNPJ14 = [
        "des_nome_cliente_razao_social",
        "des_cpfcnpj14_status",
    ]
    CONTA = [
        "des_segmentacao",
        "des_conta_status",
    ]
    CARTEIRA = [
        "cod_hierarquia_gq_segmento",
        "cod_hierarquia_plataforma",
        "cod_hierarquia_gerente",
    ]


class ColsALL(Enum):
    """
    Colunas para serem utilizadas no processamento e gravadas na saída
    """

    CNPJ9 = [
        "num_cpfcnpj",
        "num_cpfcnpj14",
        "id_chave_cliente",
        "des_nome_cliente_razao_social",
        "des_cpfcnpj_status",
        # "anomesdia",
    ]
    CNPJ14 = [
        "num_cpfcnpj14",
        "num_cpfcnpj",
        "id_chave_cliente",
        "des_nome_cliente_razao_social",
        "des_cpfcnpj14_status",
        "anomesdia",
    ]

    CONTA = [
        "num_agencia",
        "num_conta",
        "num_conta_dac",
        "des_segmentacao",
        "des_conta_status",
        "anomesdia",
    ]
    CARTEIRA = [
        "cod_hierarquia_gq_segmento",
        "cod_hierarquia_plataforma",
        "cod_hierarquia_gerente",
        "anomesdia",
    ]


class ColsCACHE(Enum):
    """
    Colunas Bases do Cache
    """

    CNPJ9 = ["num_cpfcnpj", "hash"]
