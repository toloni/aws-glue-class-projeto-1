from typing import List, Dict


from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.functions import col


def get_last_partition_date(spark: SparkSession, path_mesh: str) -> int:
    """
    Data da última partição do DataMesh
    """
    df = (
        spark.read.csv(path_mesh, header=True)
        .select("anomesdia")
        .orderBy("anomesdia", ascending=False)
        .limit(1)
    )

    ultima_data = df.collect()[0][0]
    return int(ultima_data)


def get_prev_partition_date(
    spark: SparkSession, path_mesh: str, last_date_mesh: int
) -> int:
    """
    Data da maior partição do DataMesh, menor que last_date_mesh
    """
    try:
        df = (
            spark.read.csv(path_mesh, header=True)
            .select("anomesdia")
            .filter(F.col("anomesdia") < last_date_mesh)
            .orderBy("anomesdia", ascending=False)
            .limit(1)
        )
        ultima_data = df.collect()[0][0]
        return int(ultima_data)

    except Exception as e:
        print(f"Erro ao obter a penúltima data Mesh: {e}")
        return None


def get_last_date_delta(spark: SparkSession, path_mesh: str) -> int:
    """
    Maior Data encontrada no Arquivo de Delta
    """
    try:
        df = (
            spark.read.parquet(path_mesh)
            .select("data_processamento")
            .orderBy("data_processamento", ascending=False)
            .limit(1)
        )

        return df.head()[0]

    except Exception as e:
        print(f"Erro ao obter a última data delta: {e}")


def read_partitions_mesh(
    spark: SparkSession, partitions_dates: List[int], path_mesh: str
) -> Dict[int, DataFrame]:
    """
    Leiture da base do DataMesh
    """

    partitions_data = {}

    for date in partitions_dates:
        try:
            df = spark.read.csv(path_mesh, header=True)
            partitions_data[date] = df.filter(col("anomesdia") == date)

        except Exception as e:
            print(f"Erro ao ler a partição {date}: {e}")

    return partitions_data
