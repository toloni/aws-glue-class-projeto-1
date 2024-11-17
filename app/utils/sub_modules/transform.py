import logging
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

from utils.column_definitions import Base as BaseEnum, Status, ColumnDefinitions
from utils.sub_modules.load import load

# Configuração do logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ===================================================================================== #
#                       ----=    TRANSFORM >> LOAD    =-----                            #
# ===================================================================================== #
def transform_load(
    df_input: DataFrame, dict_df_cache: Dict[str, DataFrame], args: Dict
):
    output_path_cache_dict = {
        BaseEnum.CNPJ9: args["OUTPUT_S3_PATH_DELTA_CNPJ9"],
        BaseEnum.CNPJ14: args["OUTPUT_S3_PATH_DELTA_CNPJ14"],
        BaseEnum.CARTEIRA: args["OUTPUT_S3_PATH_DELTA_CARTEIRA"],
        BaseEnum.CONTA: args["OUTPUT_S3_PATH_DELTA_CONTA"],
    }

    summary = []

    bases_to_process = args["PARAM_BASES_TO_PROCESS"]

    for base_param in bases_to_process:
        base = BaseEnum[base_param]

        logger.info(f"Iniciando transformação para a base: {base.name}")
        df_transformed = __transform_data(df_input, dict_df_cache[base], base)

        output_path = output_path_cache_dict[base]
        logger.info(f"Carregando base transformada para o caminho: {output_path}")
        load(df_transformed, output_path)

        # Contar registros totais por status
        status_count = df_transformed.groupBy("status").count().collect()

        # Adicionar contagem por status ao resumo
        status_summary = {status: count for status, count in status_count}
        summary.append(f"{base.name}: {status_summary}")

        # Contagem total de registros processados
        record_count = df_transformed.count()
        logger.info(f"Base {base.name} processada com {record_count} registros.")

    # Exibindo resumo no final
    logger.info("Resumo do processamento:")
    for item in summary:
        logger.info(item)


# -------------------------------------------------------------------------------------- #
def __transform_data(
    df_mesh: DataFrame, df_cache: DataFrame, base: BaseEnum
) -> DataFrame:
    """Transforma dados das bases Mesh e Cache, aplicando filtros, joins e colunas derivadas.

    Args:
        df_mesh (DataFrame): DataFrame Mesh.
        df_cache (DataFrame): DataFrame Cache.
        base (str): Identificador da base a ser processada.

    Returns:
        DataFrame: DataFrame transformado com base nas regras definidas.
    """
    logger.info(f"Iniciando transformação de dados para a base: {base.name}")

    primary_keys = ColumnDefinitions.get_columns(base, "primary_keys")
    hash_columns = ColumnDefinitions.get_columns(base, "hash_columns")
    mesh_columns = ColumnDefinitions.get_columns(base, "mesh_columns")
    cache_columns = ColumnDefinitions.get_columns(base, "cache_columns")
    delta_columns = ColumnDefinitions.get_columns(base, "delta_columns")

    # Aplicar regras específicas da base
    df_mesh = __apply_base_specific_rules(df_mesh, base)

    # Preparar dados Mesh
    df_mesh = __prepare_mesh_data(df_mesh, primary_keys, hash_columns, mesh_columns)

    # Preparar dados Cache
    df_cache = __prepare_cache_data(df_cache, cache_columns)

    logger.info("Realizando join entre Mesh e Cache")

    # Realizar o join e aplicar transformações
    return (
        df_mesh.join(df_cache, "primary_key", "full")
        .withColumns(__when_status())
        .withColumns(__coalesce_columns(base))
        .filter(col(Status.col_name()).isNotNull())
        .withColumn("data_hora_processamento", current_timestamp())
        .select(*delta_columns)
    )


# -------------------------------------------------------------------------------------- #
def __apply_base_specific_rules(df_mesh: DataFrame, base: BaseEnum) -> DataFrame:
    """Aplica regras específicas para cada base."""
    if base == BaseEnum.CNPJ9:
        df_mesh = df_mesh.filter(col("num_cpfcnpj14").substr(-6, 6).contains("0001"))
    elif base == BaseEnum.CONTA:
        df_mesh = df_mesh.withColumn(
            "contadac",
            concat("num_conta", "num_conta_dac"),
        )
    return df_mesh


# -------------------------------------------------------------------------------------- #
def __prepare_mesh_data(
    df_mesh: DataFrame, primary_keys, hash_columns, mesh_columns
) -> DataFrame:
    """Prepara o DataFrame Mesh: remove duplicados, adiciona hash e primary key."""
    return (
        df_mesh.dropDuplicates(primary_keys)
        .withColumns(
            {
                "mesh_hash": sha2(concat(*hash_columns), 256),
                "primary_key": concat(*primary_keys),
            }
        )
        .select(*mesh_columns, "mesh_hash", "primary_key")
    )


# -------------------------------------------------------------------------------------- #
def __prepare_cache_data(df_cache: DataFrame, cache_columns) -> DataFrame:
    """Prepara o DataFrame Cache: renomeia hash e adiciona primary key."""
    return df_cache.withColumnRenamed("hash", "cache_hash").withColumn(
        "primary_key", concat(*cache_columns)
    )


# -------------------------------------------------------------------------------------- #
def __when_status() -> Dict:
    """Define a coluna de status com base na comparação de hashes."""
    return {
        Status.col_name(): when(col("cache_hash").isNull(), lit(Status.INSERT.value))
        .when(col("mesh_hash").isNull(), lit(Status.DELETE.value))
        .when(col("mesh_hash") != col("cache_hash"), lit(Status.UPDATE.value))
    }


# -------------------------------------------------------------------------------------- #
def __coalesce_columns(base: BaseEnum) -> Dict:
    """Gera um dicionário de colunas coalescidas com base no tipo especificado.

    Args:
        base (str): Nome da base a ser usada.

    Returns:
        Dict: Dicionário com as colunas coalescidas.
    """
    base_mappings = {
        BaseEnum.CNPJ9: {
            "num_cpfcnpj": coalesce(col("num_cpfcnpj"), col("numerocnpj9")),
            "des_nome_cliente_razao_social": coalesce(
                col("des_nome_cliente_razao_social"), col("nome")
            ),
            "id_chave_cliente": coalesce(
                col("id_chave_cliente"), col("empresaprincipalid")
            ),
        },
        BaseEnum.CNPJ14: {
            "id_chave_cliente": coalesce(col("id_chave_cliente"), col("id")),
            "num_cpfcnpj14": coalesce(col("num_cpfcnpj14"), col("cnpj")),
            "num_cpfcnpj": coalesce(col("num_cpfcnpj"), col("cnpj9")),
        },
        BaseEnum.CARTEIRA: {
            "cod_hierarquia_gq_segmento": coalesce(
                col("cod_hierarquia_gq_segmento"), col("segmento")
            ),
            "cod_hierarquia_plataforma": coalesce(
                col("cod_hierarquia_plataforma"), col("plataforma")
            ),
            "cod_hierarquia_gerente": coalesce(
                col("cod_hierarquia_gerente"), col("numero")
            ),
        },
        BaseEnum.CONTA: {
            "num_agencia": coalesce(col("num_agencia"), col("agencia")),
            "numeroconta": coalesce(col("contadac"), col("numeroconta")),
        },
    }

    # Adiciona a coluna de hash comum a todas as bases
    coalesce_dict = base_mappings.get(base, {})
    coalesce_dict["hash"] = coalesce(col("mesh_hash"), col("cache_hash"))

    return coalesce_dict
