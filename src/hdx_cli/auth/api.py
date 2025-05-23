import json
from datetime import datetime, timedelta
from getpass import getpass

from hdx_cli.library_api.common.exceptions import (
    HdxCliException,
    HttpException,
    LogicException,
    LoginException,
)
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.common.rest_operations import post as req_post
from hdx_cli.models import AuthInfo

logger = get_logger()


def login(hostname: str, scheme: str, username: str, *, password: str = None) -> AuthInfo:
    if not password:
        auth_info = _retry(3, _do_interactive_login, hostname, scheme, username)
        return auth_info
    return _do_login(hostname, scheme, username, password)


def _do_login(hostname: str, scheme: str, username: str, password: str) -> AuthInfo:
    url = f"{scheme}://{hostname}/config/v1/login"
    login_data = {"username": f"{username}", "password": f"{password}"}
    headers = {"Accept": "application/json"}
    try:
        result = req_post(url, body=login_data, headers=headers, timeout=15)
    except HttpException as exc:
        if exc.error_code == 401:
            raise LoginException("Invalid credentials.")
        elif exc.error_code == 403:
            raise LoginException("Forbidden: invalid credentials.")
        elif exc.error_code == 404:
            raise LogicException(f"Login URL not found: {url}")
        elif exc.error_code == 408:
            raise LogicException("Timeout exception.")
        else:
            raise LogicException(f"Unexpected error: {exc}")

    content = json.loads(result.content)
    token_expiration_time = datetime.now() + timedelta(
        seconds=content["auth_token"]["expires_in"] - (content["auth_token"]["expires_in"] * 0.05)
    )
    return AuthInfo(
        token=content["auth_token"]["access_token"],
        expires_at=token_expiration_time,
        token_type=content["auth_token"]["token_type"],
        org_id=content["orgs"][0]["uuid"],
        username=username,
        method="username",
    )


def _do_interactive_login(hostname, scheme, username) -> AuthInfo:
    password = getpass(f"Password for [{username}]: ")
    return _do_login(hostname, scheme, username, password)


def _retry(num_retries, func, *args, **kwargs):
    for tried in range(num_retries):
        try:
            return func(*args, **kwargs)
        except HdxCliException as exc:
            if tried == num_retries - 1:
                raise
            logger.error(f"Error: {exc}")
    assert False, "Unreachable code"
