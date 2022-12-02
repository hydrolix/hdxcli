from typing import Tuple, Optional, List, Any

from . import rest_operations as rest_ops
from .context import ProfileUserContext

ResourceKind = str
ResourceName = str


def access_resource_detailed(ctx: ProfileUserContext,
                             resource_kind_and_name:
                             List[Tuple[ResourceKind,
                                        Optional[ResourceName]]],
                             *,
                             base_path='') -> Tuple[Any, str]:
    """Receives a a context and a list of [(resource_kind, resource_name),...].
    It keeps building a path to access it by accumulation, with one request
    (in the future it could be cached) per pair.

    Example:

        access_resource(myctx, [('projects', 'myproject'), ('tables', 'mytable'),
         ('transforms', 'thetransform')])

    Would access first the project and find the id, continue building the path
    with /tables, find mytable... etc. until returning the transform. If
    'thetransform' is None, then return the list of transforms, otherwise
    'thetransform' resource.
    """
    profile_info = ctx
    org_id = ctx.org_id
    hostname = ctx.hostname
    token = profile_info.auth
    headers = {'Authorization': f'{token.token_type} {token.token}',
               'Accept': 'application/json'}
    resource_url = (f'https://{hostname}/config/v1/orgs/{org_id}/' if not base_path else
                    f'https://{hostname}{base_path}')
    for resource, resource_name in resource_kind_and_name:
        resource_url = f'{resource_url}{resource}/'
        resource_list = rest_ops.list(resource_url,
                                      headers=headers)
        if resource_name is None:
            return (resource_list, resource_url)
        try:
            a_resource = [r for r in resource_list if r['name'] == resource_name][0]
        except:
            print(resource, resource_name)
            print(resource_url)
            print(resource_name)
            print(resource_list)
            print(headers)
            raise
        resource_url = f'{resource_url}{a_resource["uuid"]}/'
    return (a_resource, resource_url)



def access_resource(ctx: ProfileUserContext,
                    resource_kind_and_name:
                    List[Tuple[ResourceKind,
                               Optional[ResourceName]]],
                    *,
                    base_path=''):
    return access_resource_detailed(ctx, resource_kind_and_name, base_path=base_path)[0]
