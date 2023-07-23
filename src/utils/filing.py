import json
from pathlib import Path
from typing import Union


def set_json(path: Union[str, Path],
             dictionary: dict) -> None:

    with open(path, 'w') as f:
        json.dump(dictionary, f)


def get_json(path: Union[str, Path]) -> dict:
    with open(path, 'r') as f:
        content = json.load(f)

    return content

