from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    sha2,
    concat,
    coalesce,
    lit,
    when,
    current_timestamp,
)

from utils.enum import (
    Base,
    ColumnMeshPK,
    ColumnMeshHash,
    ColumnMesh,
    ColumnCachePK,
    Status,
    ColumnDelta,
)


def transform(df_mesh: DataFrame, df_cache: DataFrame, base: str) -> DataFrame:

    if base == Base.CNPJ9.name:
        df_mesh = df_mesh.filter(col("num_cpfcnpj14").substr(-6, 6).contains("0001"))

    if base == Base.CONTA.name:
        df_mesh = df_mesh.withColumn(
            "contadac",
            concat("num_conta", "num_conta_dac"),
        )

    # MESH Incluir Hash e Montar Primary Key
    df_mesh = (
        df_mesh.dropDuplicates(ColumnMeshPK[base].value)
        .withColumns(__mesh_key_columns(base))
        .select(*ColumnMesh[base].value, "mesh_hash", "primary_key")
    )

    # CACHE Renomear Hash e Montar Primary Key
    df_cache = df_cache.withColumnRenamed("hash", "cache_hash").withColumn(
        "primary_key", concat(*ColumnCachePK[base].value)
    )

    # Join MESH x CACHE
    return (
        df_mesh.join(df_cache, "primary_key", "full")
        .withColumns(__when_status())
        .withColumns(__coalesce_columns(base))
        .filter(col(Status.col_name()).isNotNull())
        .withColumn("data_hora_processamento", current_timestamp())
        .select(*ColumnDelta[base].value)
    )


# ----- private
#
def __mesh_key_columns(base) -> Dict:
    return {
        "mesh_hash": sha2(concat(*ColumnMeshHash[base].value), 256),
        "primary_key": concat(*ColumnMeshPK[base].value),
    }


def __when_status() -> Dict:
    return {
        Status.col_name(): when(col("cache_hash").isNull(), lit(Status.INSERT.value))
        .when(col("mesh_hash").isNull(), lit(Status.DELETE.value))
        .when(col("mesh_hash") != col("cache_hash"), lit(Status.UPDATE.value))
    }


def __coalesce_columns(base: str) -> Dict:

    coalesce_dict = {}

    if base == Base.CNPJ9.name:
        coalesce_dict = {
            "num_cpfcnpj": coalesce(col("num_cpfcnpj"), col("numerocnpj9")),
            "des_nome_cliente_razao_social": coalesce(
                col("des_nome_cliente_razao_social"), col("nome")
            ),
            "id_chave_cliente": coalesce(
                col("id_chave_cliente"), col("empresaprincipalid")
            ),
        }
    elif base == Base.CNPJ14.name:
        coalesce_dict = {
            "id_chave_cliente": coalesce(col("id_chave_cliente"), col("id")),
            "num_cpfcnpj14": coalesce(col("num_cpfcnpj14"), col("cnpj")),
            "num_cpfcnpj": coalesce(col("num_cpfcnpj"), col("cnpj9")),
        }
    elif base == Base.CARTEIRA.name:
        coalesce_dict = {
            "cod_hierarquia_gq_segmento": coalesce(
                col("cod_hierarquia_gq_segmento"), col("segmento")
            ),
            "cod_hierarquia_plataforma": coalesce(
                col("cod_hierarquia_plataforma"), col("plataforma")
            ),
            "cod_hierarquia_gerente": coalesce(
                col("cod_hierarquia_gerente"), col("numero")
            ),
        }
    elif base == Base.CONTA.name:
        coalesce_dict = {
            "num_agencia": coalesce(col("num_agencia"), col("agencia")),
            "numeroconta": coalesce(col("contadac"), col("numeroconta")),
        }

    coalesce_dict["hash"] = coalesce(col("mesh_hash"), col("cache_hash"))

    return coalesce_dict
