from typing import Callable
from pathlib import Path

from .config_constants import PROFILE_CONFIG_FILE


def try_first_time_use(config_func : Callable, target_config_file):
    if Path(target_config_file).exists():
        return False
    config_func()
    return True
