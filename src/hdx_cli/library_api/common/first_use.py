from typing import Callable
from pathlib import Path

from .config_constants import PROFILE_CONFIG_FILE


def _config_file_detected():
    return Path.exists(PROFILE_CONFIG_FILE)

def try_first_time_use(config_func : Callable):
    if _config_file_detected():
        return False
    config_func()
    return True
    