from hdx_cli.auth.api import login
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.models import AuthInfo

logger = get_logger()


def _prompt_username() -> str:
    # Get username
    while True:
        logger.info("Username: [!n]")
        input_username = input("").strip()
        if is_valid_username(input_username):
            break
        logger.info("Invalid username. Please, try again.")
    return input_username


def authenticate_user(
    hostname: str, scheme: str, username: str = None, password: str = None
) -> AuthInfo:
    """
    Authenticate the user.
    """
    if not username:
        username = _prompt_username()

    auth_info = login(
        hostname,
        scheme,
        username,
        password=password,
    )
    return auth_info


def is_valid_username(username: str) -> bool:
    return username and not username[0].isdigit()
