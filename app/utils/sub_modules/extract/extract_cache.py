from awsglue.context import GlueContext
from pyspark.sql import DataFrame
from utils.enums import Base

from typing import Dict


def extract_cache(args: Dict, glueContext: GlueContext, base: Base):

    path_cache = build_path_cache(args)
    cache_dict = {}

    # Single Bases
    if base in [Base.CNPJ9, Base.CNPJ14, Base.CARTEIRA, Base.CONTA]:
        cache_dict[base] = extract_cache_s3(path_cache[base], glueContext=glueContext)
        return cache_dict

    # Linked Bases TODO

    pass


def build_path_cache(args):
    return {
        Base.CNPJ9: args["PATH_S3_CNPJ9"],
        Base.CNPJ14: args["PATH_S3_CNPJ14"],
        Base.CARTEIRA: args["PATH_S3_CARTEIRA"],
        Base.CONTA: args["PATH_S3_CONTA"],
    }


def extract_cache_s3(path: str, glueContext=GlueContext) -> DataFrame:
    spark = glueContext.spark_session
    df = spark.read.csv(path, header=True, sinferSchema=True, sep=",")
    return df
