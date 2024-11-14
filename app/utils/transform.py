from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, concat, coalesce, lit, udf, when
from pyspark.sql.types import StringType

from utils.enum import Base, Status


# Função para calcular o hash da linha
def calculate_hash(cols):
    return sha2(concat(*cols), 256)


# Função para determinar o status
def determine_status(mesh_hash, cache_hash):
    if mesh_hash != cache_hash:
        return Status.UPDATE.value
    elif cache_hash is None:
        return Status.INSERT.value
    elif mesh_hash is None:
        return Status.DELETE.value
    return None


# Registrando a UDF para determinar o status
determine_status_udf = udf(determine_status, StringType())


def transform_cnpj9(df_mesh: DataFrame, df_cache: DataFrame, base: str) -> DataFrame:
    """
    Transforma os dados para CNPJ9, com as colunas:
        id_chave_cliente, num_cpfcnpj, des_nome_cliente_razao_social,
        des_cpfcnpj_status, id_dynamics, hash, status
    """
    # Configuração das colunas de chave primária e de hash
    primary_key_columns = ["num_cpfcnpj"]
    hash_columns = [
        "id_chave_cliente",
        "des_nome_cliente_razao_social",
        "des_cpfcnpj_status",
    ]
    status_column = Status.get_col_name()

    # Filtra registros de CNPJ válidos se a base for CNPJ9
    if base == Base.CNPJ9.value:
        df_mesh = df_mesh.filter(col("num_cpfcnpj14").substr(-6, 6).contains("0001"))

    # Remove duplicatas e adiciona coluna de hash no DataFrame principal
    df_mesh_dedup = df_mesh.dropDuplicates(primary_key_columns).withColumn(
        "mesh_hash", calculate_hash(hash_columns)
    )
    df_cache = df_cache.withColumnRenamed("hash", "cache_hash")

    # Realiza o join e aplica a UDF para calcular o status
    df_transformed = (
        df_mesh_dedup.alias("mesh")
        .join(
            df_cache.alias("cache"),
            col("mesh.num_cpfcnpj") == col("cache.emp_cnpj"),
            "full",
        )
        .withColumn(
            status_column,
            when(col("mesh_hash") != col("cache_hash"), lit(Status.UPDATE.value))
            .when(col("cache_hash").isNull(), lit(Status.INSERT.value))
            .when(col("mesh_hash").isNull(), lit(Status.DELETE.value)),
        )
        .withColumn(
            "status_2", determine_status_udf(col("mesh_hash"), col("cache_hash"))
        )
        .filter(col(status_column).isNotNull())
        .withColumn(
            "num_cpfcnpj", coalesce(col("mesh.num_cpfcnpj"), col("cache.emp_cnpj"))
        )
        .withColumn("hash", coalesce(col("mesh.mesh_hash"), col("cache.cache_hash")))
        .select(
            col("mesh.id_chave_cliente"),
            "num_cpfcnpj",
            col("mesh.des_nome_cliente_razao_social"),
            col("mesh.des_cpfcnpj_status"),
            col("cache.id").alias("id_dynamics"),
            "hash",
            status_column,
            "status_2",
        )
    )

    return df_transformed
