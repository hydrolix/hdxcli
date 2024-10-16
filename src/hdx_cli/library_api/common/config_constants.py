import os
import sys
from pathlib import Path

from hdx_cli.library_api.common.logging import get_logger

logger = get_logger()

__all__ = ['HDX_CONFIG_DIR', 'PROFILE_CONFIG_FILE', 'PROFILE_CACHE_DIR']

HDX_CONFIG_DIR_DEFAULT = Path.home() / '.hdx_cli'
HDX_CONFIG_DIR_ENV = os.getenv('HDX_CONFIG_DIR')
HDX_CONFIG_DIR = Path(HDX_CONFIG_DIR_ENV) if HDX_CONFIG_DIR_ENV else HDX_CONFIG_DIR_DEFAULT

PROFILE_CONFIG_FILE = HDX_CONFIG_DIR / 'config.toml'
PROFILE_CACHE_DIR = HDX_CONFIG_DIR


if HDX_CONFIG_DIR_ENV and not HDX_CONFIG_DIR.exists():
    logger.error(f"The specified directory in 'HDX_CONFIG_DIR': {HDX_CONFIG_DIR} does not exist.")
    sys.exit(1)
