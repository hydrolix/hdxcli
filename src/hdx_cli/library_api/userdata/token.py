from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class AuthInfo:
    token: str
    expires_at: datetime
    org_id: str
    token_type: str = 'Bearer'
