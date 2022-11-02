from typing import Mapping, Any


def get_dot_separated_key(dot_sep_key: str, mapping: Mapping[str, Any]):
    dot_sep_key_parts = dot_sep_key.split('.')
    key_ref = mapping[dot_sep_key_parts[0]]
    if len(dot_sep_key_parts[0]) > 1:
        for part in dot_sep_key_parts[1:]:
            key_ref = key_ref[part]
    return key_ref
