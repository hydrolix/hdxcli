from pathlib import Path

__all__ = ['HDX_CLI_HOME_DIR', 'PROFILE_CONFIG_FILE', 'PROFILE_CACHE_DIR']

HDX_CLI_HOME_DIR = Path.home() / '.hdx_cli'
PROFILE_CONFIG_FILE = HDX_CLI_HOME_DIR / 'config.toml'
PROFILE_CACHE_DIR = HDX_CLI_HOME_DIR
