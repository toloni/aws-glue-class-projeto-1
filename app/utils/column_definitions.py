from enum import Enum


class Base(Enum):
    """Nome das Bases."""

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


class ColumnDefinitions:
    PRIMARY_KEYS = {
        Base.CNPJ9: ["num_cpfcnpj"],
        Base.CNPJ14: ["num_cpfcnpj14"],
        Base.CARTEIRA: [
            "cod_hierarquia_gq_segmento",
            "cod_hierarquia_plataforma",
            "cod_hierarquia_gerente",
        ],
        Base.CONTA: [
            "num_agencia",
            "num_conta",
            "num_conta_dac",
        ],
    }
    HASH_COLUMNS = {
        Base.CNPJ9: [
            "id_chave_cliente",
            "des_nome_cliente_razao_social",
            "des_cpfcnpj_status",
        ],
        Base.CNPJ14: [
            "des_nome_cliente_razao_social",
            "des_cpfcnpj14_status",
        ],
        Base.CONTA: [
            "des_segmentacao",
            "des_conta_status",
        ],
        Base.CARTEIRA: [
            "cod_hierarquia_gq_segmento",
            "cod_hierarquia_plataforma",
            "cod_hierarquia_gerente",
            "cod_hierarquia_regiao",
        ],
    }
    MESH_COLUMNS = {
        Base.CNPJ9: [
            "num_cpfcnpj",
            "num_cpfcnpj14",
            "id_chave_cliente",
            "des_nome_cliente_razao_social",
            "des_cpfcnpj_status",
        ],
        Base.CNPJ14: [
            "num_cpfcnpj",
            "num_cpfcnpj14",
            "id_chave_cliente",
            "des_nome_cliente_razao_social",
            "des_cpfcnpj14_status",
        ],
        Base.CARTEIRA: [
            "cod_hierarquia_gq_segmento",
            "cod_hierarquia_plataforma",
            "cod_hierarquia_gerente",
            "cod_hierarquia_regiao",
            "des_segmentacao",
        ],
        Base.CONTA: [
            "des_segmentacao",
            "num_agencia",
            "contadac",
            "des_conta_status",
        ],
    }
    CACHE_COLUMNS = {
        Base.CNPJ9: ["numerocnpj9"],
        Base.CNPJ14: ["cnpj"],
        Base.CARTEIRA: [
            "segmento",
            "plataforma",
            "numero",
        ],
        Base.CONTA: [
            "agencia",
            "numeroconta",
        ],
    }
    DELTA_COLUMNS = {
        Base.CNPJ9: [
            "id",
            "num_cpfcnpj",
            "des_nome_cliente_razao_social",
            "id_chave_cliente",
            "des_cpfcnpj_status",
            "hash",
            "status",
            "data_hora_processamento",
        ],
        Base.CNPJ14: [
            "id_chave_cliente",
            "num_cpfcnpj14",
            "cnpj9id",
            "num_cpfcnpj",
            "des_nome_cliente_razao_social",
            "des_cpfcnpj14_status",
            "hash",
            "status",
            "data_hora_processamento",
        ],
        Base.CARTEIRA: [
            "id",
            "des_segmentacao",
            "cod_hierarquia_gq_segmento",
            "cod_hierarquia_plataforma",
            "cod_hierarquia_gerente",
            "cod_hierarquia_regiao",
            "hash",
            "status",
            "data_hora_processamento",
        ],
        Base.CONTA: [
            "id",
            "des_segmentacao",
            "num_agencia",
            "numeroconta",
            "des_conta_status",
            "hash",
            "status",
            "data_hora_processamento",
        ],
    }

    @staticmethod
    def get_columns(base: Base, column_type: str):
        """Obtém as colunas para uma base específica e tipo de coluna."""
        column_map = {
            "primary_keys": ColumnDefinitions.PRIMARY_KEYS,
            "hash_columns": ColumnDefinitions.HASH_COLUMNS,
            "mesh_columns": ColumnDefinitions.MESH_COLUMNS,
            "cache_columns": ColumnDefinitions.CACHE_COLUMNS,
            "delta_columns": ColumnDefinitions.DELTA_COLUMNS,
        }
        # Acesso direto, sem usar o método get, pois é um dicionário
        return column_map[column_type].get(base, [])

    """
    Colunas Bases do Cache
    CNPJ9 - id, numerocnpj9, nome, empresaprincipalid, hash, status, datacriacao
    CNPJ14 - id, cnpj, cnpj9, cnpj9id, hash, status, datacriacao
    CARTEIRA - id, numero, plataforma, segmento, tipo, hash, status, datacriacao
    CONTA - id, agencia, numeroconta, hash, status, datacriacao
    """
