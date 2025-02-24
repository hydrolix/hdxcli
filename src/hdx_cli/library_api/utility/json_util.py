import json
import logging
from typing import Any, Mapping


def get_dot_separated_key(dot_sep_key: str, mapping: Mapping[str, Any]):
    dot_sep_key_parts = dot_sep_key.split(".")
    key_ref = mapping[dot_sep_key_parts[0]]
    if len(dot_sep_key_parts[0]) > 1:
        for part in dot_sep_key_parts[1:]:
            key_ref = key_ref[part]
    return key_ref


def http_error_pretty_format(error):
    try:
        status_code = error.error_code
        error_data = json.loads(error.message.decode("utf-8"))
    except json.JSONDecodeError as exc:
        logging.debug(f"Failed to decode JSON: {exc}")
        return f"{error}"
    except AttributeError as exc:
        logging.debug(f"Missing attribute: {exc}")
        return f"{error}"

    error_message = _find_error_messages(error_data)
    if error_message:
        return f"{error_message}"
    return f"{status_code} {error_data}"


def _find_error_messages(data, error_messages=None):
    if error_messages is None:
        error_messages = []
    if isinstance(data, dict):
        for _, value in data.items():
            _find_error_messages(value, error_messages)
    elif isinstance(data, list):
        for item in data:
            _find_error_messages(item, error_messages)
    elif isinstance(data, str):
        if not data.endswith("."):
            data += "."
        error_messages.append(data)
    return " ".join([msg.capitalize() for msg in error_messages])
