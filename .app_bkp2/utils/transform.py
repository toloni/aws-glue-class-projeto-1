from typing import Dict, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import concat, sha2, col, lit, when

from utils.dates_to_process import DateToProcess
from utils.enum import ColsPK, ColsHASH, ColsALL, ColsCACHE, Base


def transform(df_mesh: DataFrame, df_cache: DataFrame, base) -> DataFrame:
    col_pk_name = "num_cpfcnpj"  # << mesmo nome campo chave do cache
    col_hash_name = "hash"

    cols_pk = ColsPK[base].value
    cols_hash = ColsHASH[base].value
    cols_delta = ColsALL[base].value
    cols_cache = ColsCACHE[base].value

    if Base.CNPJ9.value == base:
        # df_mesh = df_mesh.withColumn("filial", col("num_cpfcnpj14").substr(-6, 6))
        df_mesh = df_mesh.filter(col("num_cpfcnpj14").substr(-6, 6).contains("0001"))

    df_left = (
        df_mesh.dropDuplicates(cols_pk)
        .withColumn(col_pk_name, concat(*cols_pk))
        .withColumn(col_hash_name, sha2(concat(*cols_hash), 256))
        .select(col_pk_name, col_hash_name, *cols_delta)
    )
    print(">> left")
    df_left.show(truncate=False)

    print(">> df_cache")
    df_cache.show(truncate=False)

    # pk, hash
    df_compared = _compare(
        col_pk_name,
        col_hash_name,
        df_left.select(col_pk_name, col_hash_name),
        df_cache.select(*cols_cache),
    )
    print(">> df_compared")
    df_compared.show(truncate=False)

    df_delta = df_left.join(
        df_compared, on=col_pk_name, how="right"
    )  # .filter(col("status").isNotNull())

    print(">> Delta")
    df_delta.show()

    #  compare
    # return

    return None


def _compare(
    pk_cols_name: str,
    compare_cols_name: str,
    # final_cols: List,
    left_df: DataFrame,
    right_df: DataFrame,
) -> DataFrame:

    print("Compare: >>> Left")
    left_df.show(truncate=False)

    print("Compare: >>> right")
    right_df.show(truncate=False)

    col_date_process = "data_processamento"
    a_left = "left"
    a_right = "prev"
    col_status = "status"
    status_insert = "I"
    status_delete = "D"
    status_update = "U"
    # left_cols = [f"{a_left}." + col for col in final_cols]
    # right_cols = [f"{a_right}." + col for col in final_cols]

    print(f"Input - Ultima Particao..: {left_df.count()}")
    print(f"Input - Particao Anterior: {right_df.count()}")

    # Comparação para identificar registros a serem inseridos
    df_insert = (
        left_df.alias(a_left)
        .join(right_df.alias(a_right), on=pk_cols_name, how="left_outer")
        .withColumn(
            col_status,
            when(
                col(f"{a_right}.{compare_cols_name}").isNull(), lit(status_insert)
            ).otherwise(lit(None)),
        )
        .filter(col(col_status).isNotNull())
        .select(pk_cols_name, "status")
        # .select(*left_cols, col_status)
        # .withColumn(col_date_process, data_processamento)
    )

    # Comparação para identificar registros a serem inseridos
    df_delete = (
        left_df.alias(a_left)
        .join(right_df.alias(a_right), on=pk_cols_name, how="right_outer")
        .withColumn(
            col_status,
            when(
                col(f"{a_left}.{compare_cols_name}").isNull(), lit(status_delete)
            ).otherwise(lit(None)),
        )
        .filter(col(col_status).isNotNull())
        .select(pk_cols_name, "status")
        # .select(*right_cols, col_status)
        # .withColumn(col_date_process, data_processamento)
    )

    # Comparação para identificar registros a serem atualizados
    df_update = (
        left_df.alias(a_left)
        .join(right_df.alias(a_right), on=pk_cols_name, how="inner")
        .withColumn(
            col_status,
            when(
                col(f"{a_left}.{compare_cols_name}")
                != col(f"{a_right}.{compare_cols_name}"),
                lit(status_update),
            ).otherwise(lit(None)),
        )
        .filter(col(col_status).isNotNull())
        .select(pk_cols_name, "status")
        # .select(*left_cols, col_status)
        # .withColumn(col_date_process, data_processamento)
    )

    print(f"Delta - Inseridos........: {df_insert.count()}")
    print(f"Delta - Deletados........: {df_delete.count()}")
    print(f"Delta - Atualizados......: {df_update.count()}")

    return df_insert.union(df_delete).union(df_update)


# =====================================

# def transform(
#     partitions_mesh_df: Dict[int, DataFrame], partitions_dates: DateToProcess, base: str
# ) -> DataFrame:
#     """
#     Transforma e compara duas versões de dados de uma base específica usando datas de referência.

#     Este método processa os dados de uma base fornecida, comparando versões de diferentes datas
#     (última e penúltima partições disponíveis). Ele identifica alterações nos registros com base
#     em colunas definidas pelos Enums `ColsPK`, `ColsCOMPARE`, e `ColsALL`, categorizando os registros
#     conforme o status a seguir:

#         - status (I): Registro presente na última partição (inserido), mas ausente na anterior.
#         - status (U): Registro presente em ambas as partições, com alterações nas colunas de comparação.
#         - status (D): Registro presente na penúltima partição (deletado), mas ausente na última.
#         - status (FULL): Indica um retorno completo dos dados (caso a data anterior seja `None`).

#     **Configuração dos Enums por base:**
#         Para que a comparação funcione corretamente, cada base deve ter colunas específicas configuradas:

#         - `ColsPK[base].value`: Lista de colunas primárias que identificam os registros (chave primária).
#         - `ColsCOMPARE[base].value`: Lista de colunas usadas para verificar mudanças nos registros.
#         - `ColsALL[base].value`: Lista de colunas finais, incluídas no DataFrame de saída.

#     **Nota:**
#         Se a data anterior (penúltima partição) não estiver disponível (`None`), o método retorna a última partição
#         completa (status `FULL`), incluindo todos os registros da base selecionada.

#     Args:
#         partitions_mesh_df (Dict[int, DataFrame]): Dicionário de partições de dados, onde cada chave representa uma
#             partição (data) e o valor é o DataFrame correspondente.
#         partitions_dates (DateToProcess): Objeto que contém métodos para acessar as datas de referência (última e
#             penúltima) para a base fornecida.
#         base (str): Nome da base de dados a ser processada, usada para selecionar as colunas apropriadas dos Enums.

#     Returns:
#         DataFrame: DataFrame com as diferenças identificadas entre as datas de referência, incluindo uma coluna de
#         status ('I', 'U', 'D' ou 'FULL').

#     Exemplo:
#         >>> transform(partitions_mesh_df, partitions_dates, 'CNPJ9')
#         DataFrame com as alterações entre as duas datas de referência para a base 'CNPJ9', contendo a coluna 'status'
#         com os valores 'I', 'U', 'D' ou 'FULL' conforme aplicável.
#     """
#     print(f"\nTransformando {base} .....")

#     last_date = partitions_dates.get_last_date(base)
#     prev_date = partitions_dates.get_prev_date(base)

#     return _delta(
#         partitions_mesh_df,
#         last_date,
#         prev_date,
#         ColsPK[base].value,
#         ColsCOMPARE[base].value,
#         ColsALL[base].value,
#     )


# def _delta(
#     partitions_mesh_df: Dict[int, DataFrame],
#     last_date: int,
#     prev_date: int,
#     pk_cols: List,
#     compare_cols: List,
#     final_cols: List,
# ) -> DataFrame:

#     data_processamento = date_format(current_timestamp(), "yyyyMMdd")
#     pk_cols_name = "pk"
#     compare_cols_name = "compare_key"

#     if prev_date is None:
#         return (
#             partitions_mesh_df[last_date]
#             .dropDuplicates(pk_cols)
#             .select(*final_cols)
#             .withColumn("status", lit("FULL"))
#             .withColumn("data_processamento", data_processamento)
#         )

#     return _compare(
#         pk_cols_name,
#         compare_cols_name,
#         final_cols,
#         data_processamento,
#         (
#             partitions_mesh_df[last_date]
#             .dropDuplicates(pk_cols)
#             .withColumn(pk_cols_name, concat(*pk_cols))
#             .withColumn(compare_cols_name, concat(*compare_cols))
#             .select(pk_cols_name, compare_cols_name, *final_cols)
#         ),
#         (
#             partitions_mesh_df[prev_date]
#             .dropDuplicates(pk_cols)
#             .withColumn(pk_cols_name, concat(*pk_cols))
#             .withColumn(compare_cols_name, concat(*compare_cols))
#             .select(pk_cols_name, compare_cols_name, *final_cols)
#         ),
#     )


# def _compare(
#     pk_cols_name: str,
#     compare_cols_name: str,
#     final_cols: List,
#     data_processamento,
#     last_df: DataFrame,
#     prev_df: DataFrame,
# ) -> DataFrame:

#     col_date_process = "data_processamento"
#     a_last = "last"
#     a_prev = "prev"
#     col_status = "status"
#     status_insert = "I"
#     status_delete = "D"
#     status_update = "U"
#     last_prefix_cols = [f"{a_last}." + col for col in final_cols]
#     prev_prefix_cols = [f"{a_prev}." + col for col in final_cols]

#     print(f"Input - Ultima Particao..: {last_df.count()}")
#     print(f"Input - Particao Anterior: {prev_df.count()}")

#     # Comparação para identificar registros a serem inseridos
#     df_insert = (
#         last_df.alias(a_last)
#         .join(prev_df.alias(a_prev), on=pk_cols_name, how="left_outer")
#         .withColumn(
#             col_status,
#             when(
#                 col(f"{a_prev}.{compare_cols_name}").isNull(), lit(status_insert)
#             ).otherwise(lit(None)),
#         )
#         .filter(col(col_status).isNotNull())
#         .select(*last_prefix_cols, col_status)
#         .withColumn(col_date_process, data_processamento)
#     )

#     # Comparação para identificar registros a serem inseridos
#     df_delete = (
#         last_df.alias(a_last)
#         .join(prev_df.alias(a_prev), on=pk_cols_name, how="right_outer")
#         .withColumn(
#             col_status,
#             when(
#                 col(f"{a_last}.{compare_cols_name}").isNull(), lit(status_delete)
#             ).otherwise(lit(None)),
#         )
#         .filter(col(col_status).isNotNull())
#         .select(*prev_prefix_cols, col_status)
#         .withColumn(col_date_process, data_processamento)
#     )

#     # Comparação para identificar registros a serem atualizados
#     df_update = (
#         last_df.alias(a_last)
#         .join(prev_df.alias(a_prev), on=pk_cols_name, how="inner")
#         .withColumn(
#             col_status,
#             when(
#                 col(f"{a_last}.{compare_cols_name}")
#                 != col(f"{a_prev}.{compare_cols_name}"),
#                 lit(status_update),
#             ).otherwise(lit(None)),
#         )
#         .filter(col(col_status).isNotNull())
#         .select(*last_prefix_cols, col_status)
#         .withColumn(col_date_process, data_processamento)
#     )

#     print(f"Delta - Inseridos........: {df_insert.count()}")
#     print(f"Delta - Deletados........: {df_delete.count()}")
#     print(f"Delta - Atualizados......: {df_update.count()}")

#     return df_insert.union(df_delete).union(df_update)
