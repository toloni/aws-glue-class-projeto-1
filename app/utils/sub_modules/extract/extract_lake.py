from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from typing import Dict


def extract_lake(args: Dict, spark: SparkSession) -> DataFrame:

    path = args["PATH_LAKE"]

    df = spark.read.csv(path, header=True, inferSchema=True, sep=",")

    return df
