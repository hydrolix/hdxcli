from datetime import datetime, timedelta
import json
from getpass import getpass


import requests as req

from ..userdata.token import AuthInfo
from .exceptions import LoginException, HdxCliException

def _do_login(username, hostname,
              password,
              *,
              use_ssl=True):
    try:
        scheme = "https" if use_ssl else "http"
        url = f'{scheme}://{hostname}/config/v1/login'
        login_data = {'username': f'{username}',
                      'password': f'{password}'}
        result = req.post(url, json=login_data,
                          headers={'Accept': 'application/json'})
    except Exception as exc:
        raise LoginException() from exc
    else:
        if result.status_code != 200:
            raise LoginException(
                f'Could not login. Error code {result.status_code}. '
                f'Message: {str(result.content, encoding="utf-8")}')
        content = json.loads(result.content)
        token_expiration_time = (datetime.now()
                                 + timedelta(seconds=content['auth_token']['expires_in'] -
                                             (content['auth_token']['expires_in']
                                              * 0.05)))
        return AuthInfo(token=content['auth_token']['access_token'],
                        expires_at=token_expiration_time,
                        token_type=content['auth_token']['token_type'],
                        org_id=content['orgs'][0]['uuid'])


def _do_interactive_login(username, hostname,
                          *,
                          use_ssl):
    password = getpass()
    return _do_login(username, hostname,
             use_ssl=use_ssl,
             password=password)


def _retry(num_retries, failed_message, func,
           *args, **kwargs):

    for tried in range(num_retries):
        try:
            return func(*args, **kwargs)
        except HdxCliException:
            print(failed_message)
            if tried == num_retries - 1:
                raise
    assert False, "Unreachable code"


def login(username, hostname,
          password=None,
          *,
          use_ssl=True):
    """Login a user given a profile"""
    if not password:
        auth_token = _retry(3, "Wrong password", _do_interactive_login,
                      username, hostname,
                      use_ssl=use_ssl)
        return auth_token
    return _do_login(username, hostname, use_ssl=use_ssl, password=password)
