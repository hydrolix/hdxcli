import json
import time
from io import BytesIO
from typing import Any, Dict, Optional, Union

import requests

from .exceptions import HttpException

Headers = Dict[str, str]
Params = Optional[Dict[str, str]]


def create(
    url: str,
    *,
    headers: Headers,
    timeout: int,
    body: Optional[Union[Dict[str, Any], bytes]] = None,
    body_type: str = "json",
    params: Params = None,
):
    request_kwargs = {"url": url, "headers": headers, "timeout": timeout, "params": params or {}}

    if body_type == "json":
        request_kwargs["json"] = body
    else:
        request_kwargs["data"] = body

    result = requests.post(**request_kwargs)
    if result.status_code not in {200, 201}:
        raise HttpException(result.status_code, result.content)

    return result


def create_file(
    url: str,
    *,
    headers: Headers,
    file_stream: BytesIO | bytes,
    remote_filename: str | None,
    timeout: int,
    params: Params = None,
):
    result = requests.post(
        url,
        files={"file": file_stream},
        data={"name": remote_filename},
        headers=headers,
        timeout=timeout,
        params=params or {},
    )

    if result.status_code not in (201, 200):
        raise HttpException(result.status_code, result.content)

    return result


def post_with_retries(
    url: str,
    data: dict,
    user: str = None,
    password: str = None,
    retries: int = 3,
    backoff_factor: float = 0.5,
    timeout: int = 30,
    *,
    params: Params = None,
):
    auth = (user, password) if user and password else None

    for attempt in range(retries):
        response = None
        try:
            response = requests.post(
                url, json=data, timeout=timeout, auth=auth, params=params or {}
            )
            response.raise_for_status()
            return response
        except requests.RequestException:
            if attempt >= retries - 1:
                return response

            sleep_time = backoff_factor * (2**attempt)
            time.sleep(sleep_time)


def update_with_patch(
    url: str, *, headers: Headers, timeout: int, body: dict, params: Params = None
):
    result = requests.patch(url, json=body, headers=headers, timeout=timeout, params=params or {})

    if result.status_code != 200:
        raise HttpException(result.status_code, result.content)

    return result


def update_with_put(url: str, *, headers: Headers, timeout: int, body: dict, params: Params = None):
    result = requests.put(url, json=body, headers=headers, timeout=timeout, params=params or {})

    if result.status_code != 200:
        raise HttpException(result.status_code, result.content)

    return result


def list(url: str, *, headers: Headers, fmt: str = "json", timeout: int, params: Params = None):
    result = requests.get(url, headers=headers, timeout=timeout, params=params or {})

    if result.status_code != 200:
        raise HttpException(result.status_code, result.content)

    if fmt == "json":
        return json.loads(result.content)

    return result.content


get = list


def options(url: str, *, headers: Headers, timeout: int):
    result = requests.options(url, headers=headers, timeout=timeout)

    if result.status_code != 200:
        raise HttpException(result.status_code, result.content)

    return json.loads(result.content)


def delete(url: str, *, headers: Headers, timeout: int, params: Params = None):
    result = requests.delete(url, headers=headers, timeout=timeout, params=params or {})

    if result.status_code != 204:
        raise HttpException(result.status_code, result.content)

    return json.loads("{}")
