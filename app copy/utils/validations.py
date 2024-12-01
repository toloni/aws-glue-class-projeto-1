from ast import Dict
from utils.enums import Base


def is_to_process_base(args: Dict, base: Base) -> bool:

    bases_to_process = args["BASES_TO_PROCESS"].split(",")

    if base.name in bases_to_process:
        return True

    return False
