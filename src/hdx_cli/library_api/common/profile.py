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
    input_hostname = None
    good_hostname = False
    input_username = None
    good_username = False
    good_scheme_choice = False
    input_http_scheme = None
    try:
        default_hostname = f'(default: {hostname})' if hostname else ''
        while not good_hostname:
            input_hostname = input(f'Please, type the host name of your cluster {default_hostname}: ')
            if not input_hostname and default_hostname:
                input_hostname = hostname
            good_hostname = is_valid_hostname(input_hostname)
            if not good_hostname:
                print('Invalid host name. Please, try again')

        assert(input_hostname)
        default_username = f'(default: {username})' if username else ''
        while not good_username:
            input_username = input(f'Please, type the user name of your cluster {default_username}: ')
            if not input_username and default_username:
                input_username = username
            good_username = is_valid_username(input_username)
            if not good_username:
                print('Invalid user name. Please, try again')

        assert(input_username)
        default_scheme = f'(default: {http_scheme})' if http_scheme else ''
        while not good_scheme_choice:
            http_scheme_choice = input(f'Will you be using https {default_scheme} (Y/N): ')
            if http_scheme_choice.lower() == 'y':
                input_http_scheme = 'https'
                good_scheme_choice = True
            elif http_scheme_choice.lower() == 'n':
                input_http_scheme = 'http'
                good_scheme_choice = True
            if default_scheme and not input_http_scheme:
                input_http_scheme = http_scheme
        assert(input_http_scheme)
    except KeyboardInterrupt:
        return None
    else:
        return ProfileWizardInfo(hostname=input_hostname,
                                 username=input_username,
                                 scheme=input_http_scheme)


def save_profile(username:str,
                 hostname: str,
                 profile_config_file: Path,
                 profilename: str,
                 *,
                 scheme='https',
                 initial_profile={}):
    profile_to_save = ProfileUserContext(username=username,
                                         hostname=hostname,
                                         profile_config_file=profile_config_file,
                                         profilename='default',
                                         scheme=scheme)
    config_data = {profilename: profile_to_save.as_dict_for_config()}
    initial_profile.update(config_data)
    os.makedirs(Path(profile_config_file).parent, exist_ok=True)
    with open(profile_config_file, 'w+', encoding='utf-8') as config_file:
        toml.dump(initial_profile, config_file)
