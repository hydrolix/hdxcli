import os
from pathlib import Path

import toml

from .context import ProfileUserContext


def save_profile(username:str,
                 hostname: str,
                 profile_config_file: Path,
                 profilename: str):
    profile_to_save = ProfileUserContext(username=username,
                                         hostname=hostname,
                                         profile_config_file=profile_config_file,
                                         profilename='default')

    config_data = {profilename: profile_to_save.as_dict_for_config()}
    os.makedirs(Path(profile_config_file).parent, exist_ok=True)
    with open(profile_config_file, 'w+', encoding='utf-8') as config_file:
        toml.dump(config_data, config_file)
