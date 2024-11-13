from pyspark.sql import SparkSession, DataFrame, functions as F


def get_last_partition_mesh(path_mesh, spark) -> DataFrame:
    """
    Leiture da base do DataMesh
    """

    df = spark.read.csv(path_mesh, header=True)
    return df


def get_cache_bases(path_mesh, spark) -> DataFrame:
    """
    Leiture da base do DataMesh
    """

    df = spark.read.csv(path_mesh, header=True)
    return df
