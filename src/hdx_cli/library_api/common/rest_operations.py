from typing import Dict, Any
from io import StringIO 
import json
import requests

from .exceptions import HdxCliException, HttpException

Headers = Dict[str, str]

MAX_TIMEOUT = 30


def create(url: str, *,
           headers: Headers,
           body: Dict[str, Any],
           body_type='json',
           timeout=MAX_TIMEOUT):
    if body_type == 'json':
        result = requests.post(url, json=body,
                               headers=headers,
                               timeout=MAX_TIMEOUT)
    else:
        result = requests.post(url, data=body,
                               headers=headers,
                               timeout=timeout)

    if result.status_code not in (201, 200):
        raise HttpException(result.status_code, result.content)


def create_file(url: str, *,
                headers: Headers,
                file_stream,
                remote_filename,
                timeout=MAX_TIMEOUT):
    result = requests.post(url, files={'file': file_stream}, data={'name': remote_filename},
                           headers=headers,
                           timeout=timeout)

    if result.status_code not in (201, 200):
        raise HttpException(result.status_code, result.content)


def update_with_patch(url, *,
                      headers,
                      body):
    result = requests.patch(url,
                          json=body,
                          headers=headers,
                          timeout=MAX_TIMEOUT)
    if result.status_code != 200:
        raise HttpException(result.status_code, result.content)


def update_with_put(url, *,
                    headers,
                    body):
    result = requests.put(url,
                          json=body,
                          headers=headers,
                          timeout=MAX_TIMEOUT)
    if result.status_code != 200:
        raise HttpException(result.status_code, result.content)


def list(url, *,
         headers,
         fmt='json'):
    result = requests.get(url,
                          headers=headers,
                          timeout=MAX_TIMEOUT)
    if result.status_code != 200:
        raise HttpException(result.status_code, result.content)
    if fmt == 'json':
        return json.loads(result.content)
    return result.content


get = list

def options(url, *,
            headers):
    result = requests.options(url,
                          headers=headers,
                          timeout=MAX_TIMEOUT)
    if result.status_code != 200:
        raise HttpException(result.status_code, result.content)
    return json.loads(result.content)


def delete(url, *,
           headers):
    result = requests.delete(url,
                          headers=headers,
                          timeout=MAX_TIMEOUT)
    if result.status_code != 204:
        raise HttpException(result.status_code, result.content)
    return json.loads('{}')
