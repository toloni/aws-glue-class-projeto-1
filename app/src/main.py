import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from utils.enums import Base
from utils.sub_modules.extract.extract import extract
from utils.sub_modules.transform.transform import transform
from utils.sub_modules.load import load

from utils.validations import is_to_process_base


class ETL:
    def __init__(self, args, glueContext, df_encart_pj):
        self.args = args
        self.glueContext = glueContext
        self.df_encart_pj = df_encart_pj

    def run(self, base):
        if not is_to_process_base(self.args, base):
            return

        # extract
        df_cache_dict = extract(
            args=self.args, glueContext=self.glueContext, base_type="cache", base=base
        )

        # transform
        df_transformed_data = transform(self.df_encart_pj, df_cache_dict, base)

        # load
        df_transformed_data.show()
        return
        load(df_transformed_data, base)


def main():

    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "INPUT_DB_TABLE",
            "PATH_S3_CNPJ9",
            "PATH_S3_CNPJ14",
            "PATH_S3_CARTEIRA",
            "PATH_S3_CONTA",
            "BASES_TO_PROCESS",
        ],
    )

    sc = SparkContext()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    df_encart_pj = extract(args=args, glueContext=glueContext, base_type="lake")
    etl = ETL(args, glueContext, df_encart_pj)

    for base in Base:
        etl.run(base)

    job.commit()


if __name__ == "__main__":
    main()
