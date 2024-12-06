from enum import Enum


class Status(Enum):
    INSERT = "I"
    DELETE = "D"
    UPDATE = "U"
    NO_CHANGE = "N"


class Base(Enum):
    CNPJ9 = "01"
    CNPJ14 = "02"
    CARTEIRA = "03"
    CONTA = "04"
    CNPJ9_CARTEITA = "05"
    CONTA_CLIENTE = "06"
    CLIENTE_CARTEIRA = "07"
    CARTEIRA_CONTA = "08"
