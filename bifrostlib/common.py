# Common helper functions
from typing import TextIO, Pattern, Dict
import re
import yaml


def get_group_from_file(pattern: Pattern, source: TextIO = "", buffer: str = None, group: int = 1) -> str:
    """Gets the group from a regex search against a file or buffer.

    Args:
        pattern (re.Pattern): regex style pattern with at least one capture group
        source (str, optional): File to look into. Defaults to "".
        buffer (str, optional): buffer to look into. Defaults to None.
        group (int, optional): capture group you want the result of. Defaults to 1.

    Notes: If buffer and source are provided only buffer is used

    Returns:
        str: The value of the capture group in the regex, if it's not found return None
    """
    if buffer is None:
        with open(source, "r+") as fh:
            buffer = fh.read()
    try:
        value = re.search(pattern, buffer, re.MULTILINE).group(group)
        return value
    except AttributeError:
        return None

def get_yaml(source: TextIO) -> Dict:
    """Helper function to open a yaml file and return contents

    Args:
        source (TextIO): Yaml file to open

    Returns:
        Dict: content of yaml file
    """
    with open(source) as file:
        return yaml.load(file, Loader=yaml.FullLoader)