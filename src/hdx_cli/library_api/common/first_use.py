from typing import Callable
from pathlib import Path

from .config_constants import PROFILE_CONFIG_FILE
from .logging import get_logger
from .profile import get_profile_data_from_standard_input, save_profile

logger = get_logger()


def is_first_time_use(target_config_file=PROFILE_CONFIG_FILE):
    return not Path(target_config_file).exists()


def try_first_time_use(config_func: Callable, target_config_file=PROFILE_CONFIG_FILE):
    if Path(target_config_file).exists():
        return False
    config_func(target_config_file)
    return True


def first_time_use_config(profile_config_file=PROFILE_CONFIG_FILE):
    """
    This function is called when the user is running the CLI for the first time.
    """
    logger.info(f'{" HDXCLI Init ":=^50}')
    logger.info('A new configuration will be created now.')
    logger.info('')
    profile_wizard_info = get_profile_data_from_standard_input()
    if not profile_wizard_info:
        logger.info('Configuration creation aborted')
        return
    save_profile(profile_wizard_info.username,
                 profile_wizard_info.hostname,
                 'default',
                 profile_config_file=profile_config_file,
                 scheme=profile_wizard_info.scheme)
    logger.info('')
    logger.info(f'Your configuration with profile [default] has been created at {profile_config_file}')
    logger.info('-' * 100)
