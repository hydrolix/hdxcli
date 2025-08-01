import json

import click


def read_json_from_file(path: str) -> dict:
    """Reads and decodes a JSON file."""
    try:
        with open(path, "r", encoding="utf-8") as json_file:
            return json.load(json_file)
    except FileNotFoundError as e:
        raise click.BadParameter(f"File '{path}' not found.") from e
    except json.JSONDecodeError as e:
        raise click.BadParameter(f"Error decoding JSON from file '{path}'.") from e


def read_plain_file(path: str) -> str:
    """Reads a plain text file."""
    try:
        return open(path, "r", encoding="utf-8").read()
    except FileNotFoundError as e:
        raise click.BadParameter(f"File '{path}' not found.") from e
    except IOError as e:
        raise click.BadParameter(f"Error reading from file '{path}'.") from e


def read_bytes_from_file(path: str) -> bytes:
    """Reads a file in binary mode."""
    try:
        with open(path, "rb") as data_file:
            return data_file.read()
    except FileNotFoundError as e:
        raise click.BadParameter(f"File '{path}' not found.") from e
    except IOError as e:
        raise click.BadParameter(f"Error reading from file '{path}'.") from e
