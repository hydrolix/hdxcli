from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from ..userdata.token import AuthInfo
from ..common.exceptions import ProfileNotFoundException


@dataclass
class ProfileLoadContext:
    profilename: str
    profile_config_file: Optional[Path] = None


_PROFILE_USER_CONTEXT_SAVE_FIELDS = ['username', 'hostname',
                                     'projectname', 'tablename',
                                     'scheme']

DEFAULT_TIMEOUT = 30


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
    storagename: Optional[str] = None
    kafkaname: Optional[str] = None
    kinesisname: Optional[str] = None
    siemname: Optional[str] = None
    summaryname: Optional[str] = None
    poolname: Optional[str] = None
    useremail: Optional[str] = None
    rolename: Optional[str] = None
    scheme: str = 'https'
    timeout: int = DEFAULT_TIMEOUT

    def as_dict_for_config(self):
        dict_to_save = {}
        for field_name in _PROFILE_USER_CONTEXT_SAVE_FIELDS:
            if attr_val := getattr(self, field_name):
                dict_to_save[field_name] = attr_val
        return dict_to_save

    @staticmethod
    def update_context(user_profile, **kwargs):
        """
            Method used to update variables within the user context
        """
        if not user_profile:
            raise ProfileNotFoundException('Profile not found')

        for key, value in kwargs.items():
            if hasattr(user_profile, key) and value is not None:
                setattr(user_profile, key, value)
