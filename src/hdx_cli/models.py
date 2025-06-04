from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from hdx_cli.library_api.common.exceptions import ProfileNotFoundException

_PROFILE_USER_CONTEXT_SAVE_FIELDS = ["hostname", "scheme", "projectname", "tablename"]
DEFAULT_TIMEOUT = 30


@dataclass()
class AuthInfo:
    """Holds the active authentication token and related details."""

    token: str
    expires_at: datetime
    org_id: Optional[str]
    username: Optional[str] = None
    method: str = "username"
    token_type: str = "Bearer"


@dataclass
class BasicProfileConfig:
    hostname: str
    scheme: str


@dataclass
class ProfileLoadContext:
    name: str
    config_file: Path


@dataclass
class ProfileUserContext:
    """
    Represents the current user context where a user performs operations.
    A context is populated from a LoadContext.
    """

    profile_context: ProfileLoadContext
    profile_config: BasicProfileConfig
    auth: Optional[AuthInfo] = None
    timeout: int = DEFAULT_TIMEOUT
    projectname: Optional[str] = None
    tablename: Optional[str] = None
    transformname: Optional[str] = None
    viewname: Optional[str] = None
    batchname: Optional[str] = None
    altername: Optional[str] = None
    functionname: Optional[str] = None
    dictionaryname: Optional[str] = None
    storagename: Optional[str] = None
    kafkaname: Optional[str] = None
    kinesisname: Optional[str] = None
    siemname: Optional[str] = None
    poolname: Optional[str] = None
    useremail: Optional[str] = None
    rolename: Optional[str] = None
    credentialname: Optional[str] = None
    service_accountname: Optional[str] = None

    @property
    def profilename(self) -> str:
        return self.profile_context.name

    @property
    def hostname(self) -> str:
        return self.profile_config.hostname

    @property
    def scheme(self) -> str:
        return self.profile_config.scheme

    @property
    def profile_config_file(self) -> Path:
        return self.profile_context.config_file

    @property
    def org_id(self) -> Optional[str]:
        if self.auth:
            return self.auth.org_id
        return None

    @property
    def username(self) -> Optional[str]:
        if self.auth and self.auth.username:
            return self.auth.username
        return None

    @property
    def token(self) -> Optional[str]:
        if self.auth:
            return self.auth.token
        return None

    @property
    def token_expiration(self) -> Optional[datetime]:
        if self.auth:
            return self.auth.expires_at
        return None

    @property
    def token_type(self) -> Optional[str]:
        if self.auth:
            return self.auth.token_type
        return None

    @property
    def method(self) -> Optional[str]:
        if self.auth:
            return self.auth.method
        return None

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
            raise ProfileNotFoundException("Profile not found.")

        for key, value in kwargs.items():
            if hasattr(user_profile, key) and value is not None:
                setattr(user_profile, key, value)

    @classmethod
    def from_flat_dict(cls, data: dict[str, Any]) -> "ProfileUserContext":
        plc_data = {
            "name": data.get("profile_name"),
            "config_file": data.get("profile_config_file"),
        }

        if plc_data["config_file"] and not isinstance(plc_data["config_file"], Path):
            plc_data["config_file"] = Path(plc_data["config_file"])

        # Validate profile name
        if not plc_data["name"]:
            raise ValueError("There was a problem obtaining the profile name.")

        load_context_obj = ProfileLoadContext(**plc_data)

        # Gets the basic profile config data
        bpc_data = {"hostname": data.get("hostname"), "scheme": data.get("scheme")}
        if not bpc_data["hostname"]:
            raise ValueError("There was a problem obtaining the hostname.")
        basic_config_obj = BasicProfileConfig(**bpc_data)

        # Gets the auth data
        auth_obj: Optional[AuthInfo] = None
        auth_data = data.get("auth")
        if isinstance(auth_data, dict):
            auth_obj = AuthInfo(**auth_data)
        elif isinstance(auth_data, AuthInfo):
            auth_obj = auth_data

        # Validate token expiration type
        if auth_obj and isinstance(auth_obj.expires_at, str):
            auth_obj.expires_at = datetime.fromisoformat(auth_obj.expires_at)

        puc_args = {
            "profile_context": load_context_obj,
            "profile_config": basic_config_obj,
            "auth": auth_obj,
            "timeout": DEFAULT_TIMEOUT,
            "projectname": data.get("projectname"),
            "tablename": data.get("tablename"),
        }

        return cls(**puc_args)
