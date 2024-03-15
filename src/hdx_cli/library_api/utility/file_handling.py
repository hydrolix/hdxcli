import click
import json


def load_json_settings_file(ctx, param, value):
    if value is None:
        return None
    try:
        with open(value, 'r') as json_file:
            return json.load(json_file)
    except FileNotFoundError as e:
        raise click.BadParameter(f"File '{value}' not found.") from e
    except json.JSONDecodeError as e:
        raise click.BadParameter(f"Error decoding JSON from file '{value}'.") from e


def load_plain_file(ctx, param, value):
    if value is None:
        return None
    try:
        return open(value, 'r').read()
    except FileNotFoundError as e:
        raise click.BadParameter(f"File '{value}' not found.") from e
    except IOError as e:
        raise click.BadParameter(f"Error reading from file '{value}'.") from e

