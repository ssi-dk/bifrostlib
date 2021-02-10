# Common helper functions
from io import FileIO
from typing import TextIO, Pattern, Dict
import re
import yaml
import os

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
        value = str(re.search(pattern, buffer, re.MULTILINE).group(group))
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


def save_yaml(data: Dict, outfile: TextIO, ) -> None:
    with open(outfile, "w") as fh:
        yaml.dump(data, fh, default_flow_style=False)


def replace(data, match, repl):
    if isinstance(data, dict):
        return {k: replace(v, match, repl) for k, v in data.items() if k == "$oid"}
    elif isinstance(data, list):
        return [replace(i, match, repl) for i in data]
    else:
        return repl if data == match else data

def mask_on_json_key(_json: Dict, mask_key=None) -> None:
    # Note this changes the object being passed so use a deep copy if you want original
    for key, value in _json.items():
        if key == mask_key:
            _json[key] = "MASKED"
        if isinstance(value, Dict):
            mask_on_json_key(value)

def mask_for_tests(_json: Dict) -> None:
    mask_on_json_key(_json, mask_key="$oid")
    mask_on_json_key(_json, mask_key="$date")

def json_key_cleaner(key: str) -> str:
    #Removes directories, and replaces .
    return key.split("/")[-1].replace(".", "_").replace(" ", "_")

from bifrostlib.datahandling import Sample
from bifrostlib.datahandling import SampleComponent
def set_status_and_save(sample: Sample, samplecomponent: SampleComponent, status:str) -> None:
    samplecomponent['status'] = status
    sample.set_component_status(samplecomponent.component, status)
    samplecomponent.save()
    sample.save()

from bifrostlib import datahandling
def date_now():
    return datahandling.date_now()