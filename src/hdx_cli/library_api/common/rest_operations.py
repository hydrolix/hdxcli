import json
import requests

from .exceptions import HdxCliException


def create(url, *,
           headers,
           body):
    result = requests.post(url, json=body,
                           headers=headers)
    if result.status_code not in (201, 200):
        raise HdxCliException(f'Error creating: {result.status_code} ' +
                              f'Message: {result.content}')

def update_with_patch(url, *,
           headers,
           body):
    result = requests.patch(url,
                          json=body,
                          headers=headers)
    if result.status_code != 200:
        raise HdxCliException(f'Error updating: {result.status_code} ' +
                              f'Message: {result.content}')

def update_with_put(url, *,
           headers,
           body):
    result = requests.put(url,
                          json=body,
                          headers=headers)
    if result.status_code != 200:
        raise HdxCliException(f'Error updating: {result.status_code} ' +
                              f'Message: {result.content}')


def list(url, *,
         headers):
    result = requests.get(url,
                          headers=headers)
    if result.status_code != 200:
        raise HdxCliException(f'Error listing: {result.status_code} ' +
                              f'Message: {result.content}')
    return json.loads(result.content)


def options(url, *,
         headers):
    result = requests.options(url,
                          headers=headers)
    if result.status_code != 200:
        raise HdxCliException(f'Error listing: {result.status_code} ' +
                              f'Message: {result.content}')
    return json.loads(result.content)


def delete(url, *,
           headers):
    result = requests.delete(url,
                          headers=headers)
    if result.status_code != 204:
        raise HdxCliException(f'Error deleting: {result.status_code} ' +
                              f'Message: {result.content}')
    return json.loads('{}')
