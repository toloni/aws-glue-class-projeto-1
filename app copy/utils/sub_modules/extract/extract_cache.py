from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from utils.enums import Base

from typing import Dict


# ------------------------------------------------------------------------- #
def extract_cache(args: Dict, spark: SparkSession, base: Base):

    path_cache = build_path_cache(args)
    cache_dict = {}

    if base == Base.CNPJ9:
        cache_dict[Base.CNPJ9] = extract_cache_s3(path_cache[Base.CNPJ9], spark=spark)
        return cache_dict

    elif base == Base.CNPJ14:
        cache_dict[Base.CNPJ14] = extract_cache_s3(path_cache[Base.CNPJ14], spark=spark)
        return cache_dict

    elif base == Base.CARTEIRA:
        cache_dict[Base.CARTEIRA] = extract_cache_s3(
            path_cache[Base.CARTEIRA], spark=spark
        )
        return cache_dict

    elif base == Base.CONTA:
        cache_dict[Base.CONTA] = extract_cache_s3(path_cache[Base.CONTA], spark=spark)
        return cache_dict

    pass


# --------------------------------------------------------------------------#
def build_path_cache(args):
    return {
        Base.CNPJ9: args["PATH_S3_CNPJ9"],
        Base.CNPJ14: args["PATH_S3_CNPJ14"],
        Base.CARTEIRA: args["PATH_S3_CARTEIRA"],
        Base.CONTA: args["PATH_S3_CONTA"],
    }


# ------------------------------------------------------------------------- #
def extract_cache_s3(path: str, spark=SparkSession) -> DataFrame:

    df = spark.read.csv(path, header=True, inferSchema=True, sep=",")
    return df
