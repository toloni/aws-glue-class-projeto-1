from pyspark.sql import SparkSession
from utils.enums import Base
from utils.sub_modules.extract.extract import extract
from utils.sub_modules.transform.transform import transform
from utils.sub_modules.load import load

from utils.validations import is_to_process_base


def get_args():

    return {
        "ENV": "local",
        "BASES_TO_PROCESS": "CONTA",
        "PATH_LAKE": "data//input//cliente.csv",
        "PATH_S3_CNPJ9": "data//input//cache_cnpj9.csv",
        "PATH_S3_CNPJ14": "data//input//cache_cnpj14.csv",
        "PATH_S3_CARTEIRA": "data//input//cache_carteira.csv",
        "PATH_S3_CONTA": "data//input//cache_conta.csv",
    }


def etl(args, spark, base, df_encart_pj):

    if not is_to_process_base(args, base):
        return

    df_cache_dict = extract(args=args, spark=spark, base_type="cache", base=base)
    df_transformed_data = transform(df_encart_pj, df_cache_dict, base)
    df_transformed_data.show()
    return
    load(df_transformed_data, base)

    pass


def main():

    args = get_args()

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("Encarteiramento Delta")
        .getOrCreate()
    )

    df_encart_pj = extract(args=args, spark=spark, base_type="lake")

    for base in Base:
        etl(args, spark, base, df_encart_pj)


if __name__ == "__main__":
    main()
