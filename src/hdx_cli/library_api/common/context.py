from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from ..userdata.token import AuthInfo


@dataclass
class ProfileLoadContext:
    profilename : str
    profile_config_file : Optional[Path] = None


_PROFILE_USER_CONTEXT_SAVE_FIELDS = ['username', 'hostname',
                                     'projectname', 'tablename',
                                     'scheme']

@dataclass
class ProfileUserContext:
    """Represents the current user context where a user performs operations.
    A context is populated from a LoadContext.
    """
    username: str
    hostname: str
    profilename: str
    profile_config_file: Path
    org_id: Optional[str] = None
    auth: Optional[AuthInfo] = None
    projectname: Optional[str] = None
    tablename: Optional[str] = None
    transformname: Optional[str] = None
    batchname: Optional[str] = None
    functionname: Optional[str] = None
    dictionaryname : Optional[str] = None
    kafkaname: Optional[str] = None
    kinesisname: Optional[str] = None
    scheme: str = 'https'

    def as_dict_for_config(self):
        dict_to_save = {}
        for field_name in _PROFILE_USER_CONTEXT_SAVE_FIELDS:
            if attr_val := getattr(self, field_name):
                dict_to_save[field_name] = attr_val
        return dict_to_save
