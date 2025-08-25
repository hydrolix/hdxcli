import json
import os

import click


def load_json_settings_file(ctx, param, value):
    if value is None:
        return None
    try:
        with open(value, "r", encoding="utf-8") as json_file:
            return json.load(json_file)
    except FileNotFoundError as e:
        raise click.BadParameter(f"File '{value}' not found.") from e
    except json.JSONDecodeError as e:
        raise click.BadParameter(f"Error decoding JSON from file '{value}'.") from e


def load_plain_file(ctx, param, value):
    if value is None:
        return None
    try:
        return open(value, "r", encoding="utf-8").read()
    except FileNotFoundError as e:
        raise click.BadParameter(f"File '{value}' not found.") from e
    except IOError as e:
        raise click.BadParameter(f"Error reading from file '{value}'.") from e


def load_bytes_file(ctx, param, value):
    if value is None:
        return None
    try:
        with open(value, "rb") as data_file:
            return data_file.read()
    except FileNotFoundError as e:
        raise click.BadParameter(f"File '{value}' not found.") from e
    except IOError as e:
        raise click.BadParameter(f"Error reading from file '{value}'.") from e


def write_bytes_to_file(file_path: str, content: bytes):
    """Writes bytes content to a specified file path."""
    try:
        # Ensure the target directory exists before writing
        parent_dir = os.path.dirname(file_path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)

        with open(file_path, "wb") as data_file:
            data_file.write(content)
    except IOError as e:
        raise click.BadParameter(f"Error writing to file '{file_path}'.") from e
    except Exception as e:
        raise click.ClickException(f"An unexpected error occurred while writing file: {e}")
