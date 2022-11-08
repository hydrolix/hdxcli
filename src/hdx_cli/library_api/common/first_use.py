from typing import Callable
from pathlib import Path


def try_first_time_use(config_func : Callable, target_config_file):
    if Path(target_config_file).exists():
        return False
    config_func(target_config_file)
    return True
