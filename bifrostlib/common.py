# Common helper functions
from re import MULTILINE
from typing import Object
import re

def get_group_from_file(pattern: re.Pattern, source: str, group=1) -> str:
    with open(source, "r+") as fh:
        buffer = fh.read()
    return re.search(pattern, buffer, re.MULTILINE).group(group)