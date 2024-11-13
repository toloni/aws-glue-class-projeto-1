from pyspark.sql import DataFrame


def load(df: DataFrame, base: str, path: str):

    print(f"Carregado {base} .....: {df.count()}")
    # df.show(truncate=False)

    # Write Parquet
    df.write.mode("append").parquet(path)
