from enum import Enum


class Base(Enum):
    """
    Nome das Bases
    """

    CNPJ9 = "CNPJ9"
    CNPJ14 = "CNPJ14"
    CONTA = "CONTA"
    CARTEIRA = "CARTEIRA"


class Status(Enum):
    """
    Dominio para coluna status
    """

    INSERT = "I"
    UPDATE = "U"
    DELETE = "D"

    def col_name():
        return "status"


# ============================================================================ #
#  MESH COLUMNS                                                                #
# ============================================================================ #
class ColumnMeshPK(Enum):
    """
    Colunas Chaves para utilização no Join
    """

    CNPJ9 = ["num_cpfcnpj"]
    CNPJ14 = ["num_cpfcnpj14"]
    CARTEIRA = [
        "cod_hierarquia_gq_segmento",
        "cod_hierarquia_plataforma",
        "cod_hierarquia_gerente",
    ]
    CONTA = [
        "num_agencia",
        "num_conta",
        "num_conta_dac",
    ]


class ColumnMeshHash(Enum):
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
        "cod_hierarquia_regiao",
    ]


class ColumnMesh(Enum):
    """
    Colunas para serem utilizadas no processamento e gravadas na saída
    """

    CNPJ9 = [
        "num_cpfcnpj",
        "num_cpfcnpj14",
        "id_chave_cliente",
        "des_nome_cliente_razao_social",
        "des_cpfcnpj_status",
    ]
    CNPJ14 = [
        "num_cpfcnpj",
        "num_cpfcnpj14",
        "id_chave_cliente",
        "des_nome_cliente_razao_social",
        "des_cpfcnpj14_status",
    ]
    CARTEIRA = [
        "cod_hierarquia_gq_segmento",
        "cod_hierarquia_plataforma",
        "cod_hierarquia_gerente",
        "cod_hierarquia_regiao",
        "des_segmentacao",
    ]
    CONTA = [
        "des_segmentacao",
        "num_agencia",
        "contadac",
        "des_conta_status",
    ]


# ============================================================================ #
#  CACHE COLUMNS                                                               #
# ============================================================================ #
class ColumnCachePK(Enum):
    """
    Colunas Bases do Cache
    CNPJ9 - id, numerocnpj9, nome, empresaprincipalid, hash, status, datacriacao
    CNPJ14 - id, cnpj, cnpj9, cnpj9id, hash, status, datacriacao
    CARTEIRA - id, numero, plataforma, segmento, tipo, hash, status, datacriacao
    CONTA - id, agencia, numeroconta, hash, status, datacriacao
    """

    CNPJ9 = ["numerocnpj9"]
    CNPJ14 = ["cnpj"]
    CARTEIRA = [
        "segmento",
        "plataforma",
        "numero",
    ]
    CONTA = [
        "agencia",
        "numeroconta",
    ]


# ============================================================================ #
#  DELTA  COLUMNS                                                              #
# ============================================================================ #
class ColumnDelta(Enum):
    CNPJ9 = [
        "id",
        "num_cpfcnpj",
        "des_nome_cliente_razao_social",
        "id_chave_cliente",
        "des_cpfcnpj_status",
        "hash",
        "status",
        "data_hora_processamento",
    ]
    CNPJ14 = [
        "id_chave_cliente",
        "num_cpfcnpj14",
        "cnpj9id",
        "num_cpfcnpj",
        "des_nome_cliente_razao_social",
        "des_cpfcnpj14_status",
        "hash",
        "status",
        "data_hora_processamento",
    ]
    CARTEIRA = [
        "id",
        "des_segmentacao",
        "cod_hierarquia_gq_segmento",
        "cod_hierarquia_plataforma",
        "cod_hierarquia_gerente",
        "cod_hierarquia_regiao",
        "hash",
        "status",
        "data_hora_processamento",
    ]
    CONTA = [
        "id",
        "des_segmentacao",
        "num_agencia",
        "numeroconta",
        "des_conta_status",
        "hash",
        "status",
        "data_hora_processamento",
    ]
