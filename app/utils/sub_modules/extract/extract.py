from utils.sub_modules.extract.extract_lake import extract_lake
from utils.sub_modules.extract.extract_cache import extract_cache


def extract(args, spark, base_type, base=None):

    if base_type == "lake":
        return extract_lake(args, spark)

    if base_type == "cache":
        return extract_cache(args=args, spark=spark, base=base)

    pass
