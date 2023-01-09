from dataclasses import dataclass
from typing import Optional

import os
from pathlib import Path

import toml

from .context import ProfileUserContext
from .validation import is_valid_username, is_valid_hostname

from hdx_cli.library_api.common.validation import is_valid_username, is_valid_hostname


@dataclass
class ProfileWizardInfo:
    hostname: str
    username: str
    scheme: str


def get_profile_data_from_standard_input(hostname: Optional[str] = None,
                                         username: Optional[str] = None,
                                         http_scheme: Optional[str] = None) -> Optional[ProfileWizardInfo]:
    good_hostname = False
    good_username = False
    good_scheme_choice = False
    try:
        default_hostname = f'(default: {hostname})' if hostname else ''
        while not good_hostname:
            hostname = input(f'Please, type the host name of your cluster {default_hostname}: ')
            good_hostname = is_valid_hostname(hostname)
            if not good_hostname:
                print('Invalid host name. Please, try again')
        assert(hostname)
        default_username = f'(default: {username})' if username else ''
        while not good_username:
            username = input(f'Please, type the user name of your cluster {default_username}: ')
            good_username = is_valid_username(username)
        assert(username)
        default_scheme = f'(default: {http_scheme})' if http_scheme else ''
        while not good_scheme_choice:
            http_scheme_choice = input(f'Will you be using https {default_scheme} (Y/N): ')
            if http_scheme_choice.lower() == 'y':
                http_scheme = 'https'
                good_scheme_choice = True
            elif http_scheme_choice.lower() == 'n':
                http_scheme = 'http'
                good_scheme_choice = True
    except KeyboardInterrupt:
        return None
    else:
        return ProfileWizardInfo(hostname=hostname,
                                 username=username,
                                 scheme=http_scheme)


def save_profile(username:str,
                 hostname: str,
                 profile_config_file: Path,
                 profilename: str,
                 *,
                 scheme='https'):
    profile_to_save = ProfileUserContext(username=username,
                                         hostname=hostname,
                                         profile_config_file=profile_config_file,
                                         profilename='default',
                                         scheme=scheme)

    config_data = {profilename: profile_to_save.as_dict_for_config()}
    os.makedirs(Path(profile_config_file).parent, exist_ok=True)
    with open(profile_config_file, 'w+', encoding='utf-8') as config_file:
        toml.dump(config_data, config_file)
