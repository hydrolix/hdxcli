import time
from typing import Any, BinaryIO, Dict, Optional, Tuple, Union

import requests

from .exceptions import HttpException

Headers = Dict[str, str]
Params = Optional[Dict[str, Any]]


def _request(
    method: str,
    url: str,
    *,
    headers: Headers,
    timeout: int,
    body: Optional[Union[Dict[str, Any], bytes]] = None,
    data: Optional[Union[str, bytes]] = None,
    files: Optional[Dict[str, Any]] = None,
    auth: Optional[Tuple[str, str]] = None,
    params: Params = None,
) -> requests.Response:
    """
    Private function to centralize HTTP request logic without retries.

    Args:
        method: HTTP method (e.g., 'GET', 'POST', etc.).
        url: The target URL.
        headers: Request headers.
        timeout: Timeout for the request.
        body: Request body data.
        files: Files to send with the request (used for file uploads).
        auth: Tuple (username, password) for authentication.
        params: URL parameters.

    Returns:
        The response object.

    Raises:
        HttpException: When the response status code is not as expected.
    """
    # Build request parameters
    request_kwargs = {
        "url": url,
        "headers": headers,
        "timeout": timeout,
        "auth": auth,
        "params": params or {},
    }

    # Add files or body to the request
    if files:
        request_kwargs["files"] = files

    if body:
        request_kwargs["json"] = body
    elif data:
        request_kwargs["data"] = data

    # Execute the HTTP request using the generic requests.request method
    response = requests.request(method, **request_kwargs)

    # Validate response status code:
    # For DELETE, expect 204 No Content; for other methods, expect 200 or 201.
    if method.lower() == "delete":
        if response.status_code != 204:
            raise HttpException(response.status_code, response.content)
    else:
        if response.status_code not in {200, 201}:
            raise HttpException(response.status_code, response.content)

    return response


def get(
    url: str,
    *,
    headers: Headers,
    timeout: int,
    fmt: str = "json",
    params: Params = None,
) -> Any:
    """
    Performs a GET request.

    Args:
        url: The target URL.
        headers: Request headers.
        timeout: Timeout for the request.
        fmt: Format of the response ('json' for JSON, otherwise raw content).
        params: URL parameters.

    Returns:
        The JSON-decoded response or raw content.

    Raises:
        HttpException.
    """
    response = _request("GET", url, headers=headers, timeout=timeout, params=params)
    return response.json() if fmt == "json" else response.content


def post(
    url: str,
    *,
    headers: Headers,
    timeout: int,
    body: Optional[Union[Dict[str, Any], bytes, str]] = None,
    body_type: str = "json",
    params: Params = None,
) -> requests.Response:
    """
    Performs a POST request without retries or authentication.

    Args:
        url: The target URL.
        headers: Request headers.
        timeout: Timeout for the request.
        body: Request body data.
        body_type: Type of the body ('json' to send JSON data, otherwise raw).
        params: URL parameters.

    Returns:
        Response object.

    Raises:
        HttpException.
    """
    content = {}
    if body_type == "json":
        content = {"body": body}
    elif body_type in ("bytes", "csv", "verbatim"):
        content = {"data": body}

    return _request("POST", url, headers=headers, timeout=timeout, params=params, **content)


def post_with_file(
    url: str,
    *,
    headers: Headers,
    timeout: int,
    file_content: Union[BinaryIO, bytes],
    file_name: Optional[str] = None,
    params: Params = None,
) -> Any:
    """
    Performs a POST request for file upload without retries.

    Args:
        url: The target URL.
        headers: Request headers.
        file_content: The file content as a BytesIO stream or bytes.
        file_name: The name of the file.
        timeout: Timeout for the request.
        params: URL parameters.

    Returns:
        Response object.

    Raises:
        HttpException.
    """
    # Prepare files and optional data for file upload
    files = {"file": file_content}
    data = {"name": file_name} if file_name else None

    return _request(
        "POST", url, headers=headers, timeout=timeout, data=data, files=files, params=params
    )


def post_with_retries(
    url: str,
    *,
    body: dict,
    headers: Headers = None,
    timeout: int = 30,
    auth: Optional[Tuple[str, str]] = None,
    retries: int = 3,
    backoff_factor: float = 0.5,
    params: Params = None,
) -> Any:
    """
    Performs a POST request with retry logic and authentication.
    This function attempts the request up to `retries` times with exponential backoff.

    Args:
        url: The target URL.
        body: Request body data (as a dictionary).
        headers: Request headers.
        timeout: Timeout for the request.
        auth: (user, pass) tuple for authentication.
        retries: Number of retry attempts.
        backoff_factor: Factor used for exponential backoff.
        params: URL parameters.

    Returns:
        Response object.

    Raises:
        HttpException or requests.RequestException if all retries fail.
    """
    for attempt in range(retries):
        response = None
        try:
            response = _request(
                "POST",
                url,
                headers=headers,
                timeout=timeout,
                body=body,
                params=params,
                auth=auth,
            )
            response.raise_for_status()
            return response
        except (HttpException, requests.RequestException):
            if attempt >= retries - 1:
                return response

            sleep_time = backoff_factor * (2**attempt)
            time.sleep(sleep_time)


def options(
    url: str,
    *,
    headers: Headers,
    timeout: int,
    params: Params = None,
) -> Any:
    """
    Performs an OPTIONS request.

    Args:
        url: The target URL.
        headers: Request headers.
        timeout: Timeout for the request.
        params: URL parameters.

    Returns:
        Response object.

    Raises:
        HttpException.
    """
    return _request("OPTIONS", url, headers=headers, timeout=timeout, params=params)


def delete(
    url: str,
    *,
    headers: Headers,
    timeout: int,
    params: Params = None,
) -> None:
    """
    Performs a DELETE request.

    Args:
        url: The target URL.
        headers: Request headers.
        timeout: Timeout for the request.
        params: URL parameters.

    Returns:
        None.

    Raises:
        HttpException.
    """
    _request("DELETE", url, headers=headers, timeout=timeout, params=params)


def patch(url: str, *, headers: Headers, timeout: int, body: dict, params: Params = None) -> None:
    """
    Performs a PATCH request.

    Args:
        url: The target URL.
        headers: Request headers.
        timeout: Timeout for the request.
        body: Request body data.
        params: URL parameters.

    Returns:
        None.

    Raises:
        HttpException.
    """
    _request("PATCH", url, headers=headers, timeout=timeout, body=body, params=params)


def put(url: str, *, headers: Headers, timeout: int, body: dict, params: Params = None) -> None:
    """
    Performs a PUT request.

    Args:
        url: The target URL.
        headers: Request headers.
        timeout: Timeout for the request.
        body: Request body data.
        params: URL parameters.

    Returns:
        None.

    Raises:
        HttpException.
    """
    _request("PUT", url, headers=headers, timeout=timeout, body=body, params=params)
