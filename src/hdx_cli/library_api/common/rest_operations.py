from typing import Dict, Any, Union
import json
import requests

from .exceptions import HdxCliException, HttpException

Headers = Dict[str, str]


def create(url: str, *,
           headers: Headers,
           timeout,
           body: Union[Dict[str, Any], bytes] = None,
           body_type='json'):
    if body_type == 'json':
        result = requests.post(url, json=body,
                               headers=headers,
                               timeout=timeout)
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
                timeout):
    result = requests.post(url, files={'file': file_stream}, data={'name': remote_filename},
                           headers=headers,
                           timeout=timeout)

    if result.status_code not in (201, 200):
        raise HttpException(result.status_code, result.content)


def update_with_patch(url, *,
                      headers,
                      timeout,
                      body,
                      params):
    result = requests.patch(url,
                            json=body,
                            headers=headers,
                            params=params,
                            timeout=timeout)
    if result.status_code != 200:
        raise HttpException(result.status_code, result.content)


def update_with_put(url, *,
                    headers,
                    timeout,
                    body,
                    params):
    result = requests.put(url,
                          json=body,
                          headers=headers,
                          params=params,
                          timeout=timeout)
    if result.status_code != 200:
        raise HttpException(result.status_code, result.content)


def list(url, *,
         headers,
         fmt='json',
         timeout):
    result = requests.get(url,
                          headers=headers,
                          timeout=timeout)
    if result.status_code != 200:
        raise HttpException(result.status_code, result.content)
    if fmt == 'json':
        return json.loads(result.content)
    return result.content


get = list


def options(url, *,
            headers,
            timeout):
    result = requests.options(url,
                              headers=headers,
                              timeout=timeout)
    if result.status_code != 200:
        raise HttpException(result.status_code, result.content)
    return json.loads(result.content)


def delete(url, *,
           headers,
           timeout,
           params=None):
    result = requests.delete(url,
                             headers=headers,
                             params=params,
                             timeout=timeout)
    if result.status_code != 204:
        raise HttpException(result.status_code, result.content)
    return json.loads('{}')
