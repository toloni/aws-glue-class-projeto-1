from pyspark.sql import DataFrame
from utils.enums import Base


def load(df: DataFrame, base: Base):

    df.show()
    pass
