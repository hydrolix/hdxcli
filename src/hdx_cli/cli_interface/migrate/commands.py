from pathlib import Path

import io
import os
from urllib.parse import urlparse

import click
import json
import toml

from ...library_api.common import rest_operations as lro
from ...library_api.common.auth_utils import generate_temporal_profile
from ...library_api.common.context import ProfileUserContext
from ...library_api.common.validation import is_valid_hostname, is_valid_username
from ...library_api.common.generic_resource import access_resource_detailed
from ...library_api.common.exceptions import LogicException, HttpException
from ...library_api.common.logging import get_logger
from ...library_api.utility.decorators import report_error_and_exit
from ..common.undecorated_click_commands import (basic_show,
                                                 basic_create_with_body_from_string)

from .rollback import (MigrateStatus, MigrationRollbackManager,
                       DoNothingMigrationRollbackManager, MigrationEntry, ResourceKind)

logger = get_logger()


def _setup_target_cluster_config(profile_config_file,
                                 target_cluster_username,
                                 target_cluster_hostname,
                                 target_cluster_scheme):
    username = target_cluster_username
    hostname = target_cluster_hostname
    scheme = target_cluster_scheme
    config_data = {'default': {'username': username, 'hostname': hostname, 'scheme': scheme}}
    os.makedirs(Path(profile_config_file).parent, exist_ok=True)
    with open(profile_config_file, 'w+', encoding='utf-8') as config_file:
        toml.dump(config_data, config_file)


def migrate_projects(ctx: click.Context,
                     source_profile: ProfileUserContext,
                     target_profile: ProfileUserContext,
                     whitelist=None,
                     blacklist=None):
    if whitelist and blacklist:
        raise LogicException("Black and white lists canot be provided in the same function call")
    source_projects, source_projects_url = access_resource_detailed(source_profile,
                                                                    [('projects', None)])
    source_resource_path = urlparse(source_projects_url).path
    split_source_res_path = source_resource_path.split('/')
    split_source_res_path[-3] = target_profile.org_id
    target_resource_path = '/'.join(split_source_res_path)
    for project in source_projects:
        if blacklist and project['name'] in blacklist:
            continue
        if whitelist and project['name'] not in whitelist:
            continue
        try:
            show_result = json.loads(basic_show(source_profile, source_resource_path, project['name']))
            basic_create_with_body_from_string(target_profile,
                                               target_resource_path,
                                               project['name'],
                                               json.dumps({'settings': show_result['settings']}))
        except HttpException as exc:
            if exc.error_code == 400:
                yield project, MigrateStatus.SKIPPED
            else:
                raise
        else:
            yield project, MigrateStatus.CREATED


def create_tables_for_project(project_name,
                              source_profile: ProfileUserContext,
                              target_profile: ProfileUserContext):
    source_project_tables, source_tables_url = access_resource_detailed(source_profile,
                                                                        [('projects', project_name), ('tables', None)])
    _, target_project_url = access_resource_detailed(target_profile, [('projects', project_name)])

    source_tables_path = urlparse(f'{source_tables_url}').path
    target_tables_path = urlparse(f'{target_project_url}tables/').path

    for table in source_project_tables:
        # All summary tables will be migrated at the end of the process
        if table.get('type') and table.get('type') == 'summary':
            yield table, MigrateStatus.POSTPONED
        else:
            try:
                table_show = json.loads(basic_show(source_profile, source_tables_path, table['name']))
                try:
                    table_show['settings']['autoingest'][0]['enabled'] = False

                    del table_show['settings']['autoingest'][0]['source']
                    del table_show['uuid']
                except IndexError:
                    pass
                except KeyError:
                    pass
                else:
                    basic_create_with_body_from_string(target_profile,
                                                       target_tables_path,
                                                       table['name'],
                                                       json.dumps(table_show))
            except HttpException as exc:
                if exc.error_code == 400:
                    yield table, MigrateStatus.SKIPPED
                else:
                    raise
            else:
                yield table, MigrateStatus.CREATED


def create_postponed_tables(project_name,
                            source_profile: ProfileUserContext,
                            target_profile: ProfileUserContext,
                            table):
    source_project_tables, source_tables_url = access_resource_detailed(source_profile,
                                                                        [('projects', project_name), ('tables', None)])
    _, target_project_url = access_resource_detailed(target_profile, [('projects', project_name)])

    source_tables_path = urlparse(f'{source_tables_url}').path
    target_tables_path = urlparse(f'{target_project_url}tables/').path
    try:
        table_show = json.loads(basic_show(source_profile, source_tables_path, table['name']))
        try:
            table_show['settings']['autoingest'][0]['enabled'] = False

            del table_show['settings']['autoingest'][0]['source']
            del table_show['uuid']
        except IndexError:
            pass
        except KeyError:
            pass
        else:
            basic_create_with_body_from_string(target_profile,
                                               target_tables_path,
                                               table['name'],
                                               json.dumps(table_show))
    except HttpException as exc:
        if exc.error_code == 400:
            return MigrateStatus.SKIPPED
        else:
            raise
    else:
        return MigrateStatus.CREATED


def create_functions_for_project(project_name,
                                 source_profile: ProfileUserContext,
                                 target_profile: ProfileUserContext):
    source_project_functions, source_functions_url = access_resource_detailed(source_profile,
                                                                              [('projects', project_name),
                                                                               ('functions', None)])
    _, target_function_url = access_resource_detailed(target_profile, [('projects', project_name)])

    source_functions_path = urlparse(f'{source_functions_url}').path
    target_functions_path = urlparse(f'{target_function_url}functions/').path

    for function in source_project_functions:
        try:
            function_show = json.loads(basic_show(source_profile, source_functions_path, function['name']))
           
            basic_create_with_body_from_string(target_profile,
                                               target_functions_path,
                                               function['name'],
                                               json.dumps({'sql': function_show['sql']}))
        except HttpException as exc:
            if exc.error_code == 400:
                yield function, MigrateStatus.SKIPPED
            else:
                raise
        else:
            yield function, MigrateStatus.CREATED


def create_dictionaries_for_project(project_name,
                                    source_profile: ProfileUserContext,
                                    target_profile: ProfileUserContext):
    source_project_dictionaries, _ = access_resource_detailed(source_profile,
                                                              [('projects', project_name),
                                                               ('dictionaries', None)])
    _, target_project_url = access_resource_detailed(target_profile,
                                                     [('projects', project_name)])
    target_project_path = urlparse(target_project_url).path
    target_dict_path = f'{target_project_path}dictionaries/'
    dictionary_files_so_far = set()
    for dic in source_project_dictionaries:
        d_settings = dic['settings']
        d_name = dic['name']
        d_file = d_settings['filename']
        d_format = d_settings['format']
        table_name = f'{project_name}_{d_name}'
        query_endpoint = f'{source_profile.scheme}://{source_profile.hostname}/query/?query=SELECT * FROM {table_name} FORMAT {d_format}'
        headers = {'Authorization': f'{source_profile.auth.token_type} {source_profile.auth.token}',
                   'Accept': '*/*'}
        timeout = source_profile.timeout
        contents = lro.get(query_endpoint, headers=headers, timeout=timeout, fmt='verbatim')
        try:
            if d_file not in dictionary_files_so_far:
                _create_dictionary_file_for_project(project_name, d_file, contents,
                                                    target_profile)
                dictionary_files_so_far.add(d_file)
        except HttpException as exc:
            # Dictionary file existed, no need to create it
            if exc.error_code == 400:
                yield dic, MigrateStatus.SKIPPED
            # Genuine error, abort
            else:
                raise
        finally:
            try:
                basic_create_with_body_from_string(target_profile,
                                                   target_dict_path,
                                                   d_name,
                                                   json.dumps({'settings': d_settings}))
            except HttpException as exc:
                if exc.error_code == 400:
                    yield dic, MigrateStatus.SKIPPED
                raise
            else:
                yield dic, MigrateStatus.CREATED


def _create_dictionary_file_for_project(project_name,
                                        dict_file,
                                        contents,
                                        profile: ProfileUserContext):
    _, project_url = access_resource_detailed(profile,
                                              [('projects', project_name)])

    headers = {'Authorization': f'{profile.auth.token_type} {profile.auth.token}',
               'Accept': '*/*'}
    file_url = f'{project_url}dictionaries/files/'
    timeout = profile.timeout
    lro.create_file(file_url, headers=headers,
                    file_stream=io.BytesIO(contents),
                    remote_filename=dict_file,
                    timeout=timeout)


def create_transforms_for_table(project_name,
                                table_name,
                                source_profile: ProfileUserContext,
                                target_profile: ProfileUserContext):
    source_table_transforms, source_transforms_url = access_resource_detailed(source_profile,
                                                                              [('projects', project_name),
                                                                               ('tables', table_name),
                                                                               ('transforms', None)])

    _, target_table_url = access_resource_detailed(target_profile,
                                                   [('projects', project_name),
                                                    ('tables', table_name)
                                                    ])

    source_transforms_path = urlparse(f'{source_transforms_url}').path
    target_transforms_path = urlparse(f'{target_table_url}transforms/').path

    for transform in source_table_transforms:
        transform_show = json.loads(basic_show(source_profile, source_transforms_path, transform['name']))
        try:
            del transform_show['uuid']
            basic_create_with_body_from_string(target_profile,
                                               target_transforms_path,
                                               transform['name'],
                                               json.dumps(transform_show))
        except HttpException as exc:
            logger.error(f'{exc}')
            if exc.error_code == 400:
                yield transform, MigrateStatus.SKIPPED
            else:
                raise
        else:
            yield transform, MigrateStatus.CREATED


@click.command(help="Migrate projects to a target cluster. The migrate command takes care of migrating"
               " projects, tables, transforms, dictionaries and functions. Projects can be whitelisted"
               " and blacklisted. Any project that already exists in the target cluster is considered to"
               " be the same as in the source cluster and its creation will be skipped. However, "
               " if the project has tables or other resources that exist in the source but not in the target, "
               " those resources will be created.\n"
               " If the migration fails, a rollback will happen by default. The rollback will rollback exactly "
               " the resources that were created during the migration so far. Resources that already existed in "
               " the target cluster before starting the migration will not be deleted, even if the source cluster "
               " contains those resources (resource matching between source and cluster is done by name)."
               " You can use black lists and white lists by giving repeated arguments with project names."
               " Only one of blacklisting or whitelisting is allowed in the same migration command.")
@click.argument('target_cluster_username', metavar='TARGET_CLUSTERUSERNAME', required=True, default=None)
@click.argument('target_cluster_hostname', metavar='TARGET_CLUSTERHOSTNAME', required=True, default=None)
@click.option('-p', '--target-cluster-password', required=False, default=None)
@click.option('-u', '--target-cluster-uri-scheme', required=False, default='https')
@click.option('-B', '--project-blacklist', multiple=True, default=None, required=False)
@click.option('-b', '--project-whitelist', multiple=True, default=None, required=False)
@click.option('-R', '--no-rollback', default=False)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def migrate(ctx: click.Context,
            target_cluster_username,
            target_cluster_hostname,
            target_cluster_password,
            target_cluster_uri_scheme,
            project_blacklist,
            project_whitelist,
            no_rollback):
    if project_blacklist and project_whitelist:
        raise LogicException('You can only use project whitelist or project black list but not both.')

    if not target_cluster_username or not is_valid_username(target_cluster_username):
        raise LogicException('Incorrect user name')
    if not target_cluster_hostname or not is_valid_hostname(target_cluster_hostname):
        raise LogicException('Incorrect host name')

    target_user_profile = generate_temporal_profile(target_cluster_hostname,
                                                    target_cluster_username,
                                                    target_cluster_password,
                                                    target_cluster_uri_scheme)

    logger.info(f"Cluster to migrate to is '{target_cluster_hostname}'")
    logger.info(f"User for cluster to migrate data is '{target_cluster_username}'")
    logger.info('Starting database migration. This operation can take a while...')

    if project_whitelist:
        logger.info(f'The projects white list is {project_whitelist}')
    if project_blacklist:
        logger.info(f'The projects black list is {project_blacklist}')
    logger.info('')
    if project_blacklist:
        project_blacklist = list(project_blacklist)
        project_blacklist.append('hdx')
    if project_whitelist:
        project_whitelist = list(project_whitelist)
        try:
            project_whitelist.remove('hdx')
        except ValueError:
            pass

    # Blacklist hdx, which is special
    if not project_blacklist and not project_whitelist:
        project_blacklist = ['hdx']

    mrm = MigrationRollbackManager
    if no_rollback:
        mrm = DoNothingMigrationRollbackManager
    with mrm(target_user_profile, ) as migration_rollback_manager:
        postponed_tables = []
        for project, status in migrate_projects(ctx,
                                                ctx.parent.obj['usercontext'],
                                                target_user_profile,
                                                project_whitelist,
                                                project_blacklist):
            logger.info(f'Project {project["name"]}: [!n]')
            if status == MigrateStatus.CREATED:
                logger.info('created')
                m_entry = MigrationEntry(project['name'], 
                                         ResourceKind.PROJECT)
                migration_rollback_manager.push_entry(m_entry)
            elif status == MigrateStatus.SKIPPED:
                logger.info('skipped creation (was found)')

            for func, func_status in create_functions_for_project(project['name'],
                                                                  ctx.parent.obj['usercontext'],
                                                                  target_user_profile):
                logger.info(f'\tFunction {func["name"]}: [!n]')
                if func_status == MigrateStatus.CREATED:
                    logger.info('created')
                    m_entry = MigrationEntry(func['name'], 
                                             ResourceKind.FUNCTION,
                                             [project['name']])
                    migration_rollback_manager.push_entry(m_entry)
                elif func_status == MigrateStatus.SKIPPED:
                    logger.info('skipped creation (was found)')
            for dictionary, dict_status in create_dictionaries_for_project(project['name'],
                                                             ctx.parent.obj['usercontext'],
                                                             target_user_profile):
                logger.info(f'\tDictionary {dictionary["name"]}: [!n]')
                if dict_status == MigrateStatus.CREATED:
                    logger.info('created')
                    m_entry = MigrationEntry(dictionary['name'],
                                             ResourceKind.DICTIONARY,
                                             [project['name']])
                    migration_rollback_manager.push_entry(m_entry)
                elif dict_status == MigrateStatus.SKIPPED:
                    logger.info('skipped creation (was found)')

            for table, tbl_status in create_tables_for_project(project['name'],
                                                            ctx.parent.obj['usercontext'],
                                                            target_user_profile):
                logger.info(f'\tTable {table["name"]}: [!n]')
                if tbl_status == MigrateStatus.CREATED:
                    logger.info('created')
                    m_entry = MigrationEntry(table['name'], 
                                             ResourceKind.TABLE,
                                             [project['name']])
                    migration_rollback_manager.push_entry(m_entry)
                elif tbl_status == MigrateStatus.SKIPPED:
                    logger.info('skipped creation (was found)')
                elif tbl_status == MigrateStatus.POSTPONED:
                    # The creation of the summary tables is postponed
                    # until the end of the migration process.
                    logger.info('postponed creation (summary table)')
                    postponed_tables.append((table, project['name']))
                    # If the table migrated is a summary, transforms and sources will be created automatically.
                    # Therefore, there is no need to migrate them.
                    continue

                for transform, transform_status in create_transforms_for_table(project['name'],
                                                                            table['name'],
                                                                            ctx.parent.obj['usercontext'],
                                                                            target_user_profile):
                    logger.info(f'\t\tTransform {transform["name"]}: [!n]')
                    if transform_status == MigrateStatus.CREATED:
                        logger.info('created')
                        m_entry = MigrationEntry(transform['name'], 
                                                 ResourceKind.TRANSFORM,
                                                 [project['name'], table['name']])
                        migration_rollback_manager.push_entry(m_entry)
                    elif transform_status == MigrateStatus.SKIPPED:
                        logger.info('skipped creation (was found)')
                logger.info('')

        # Postponed tables
        # Here interation a list of postponed tables to be created.
        if postponed_tables:
            logger.info('Postponed tables creation...')
            for table, project_name in postponed_tables:
                tbl_status = create_postponed_tables(project_name,
                                                     ctx.parent.obj['usercontext'],
                                                     target_user_profile,
                                                     table)
                logger.info(f'\tProject {project_name}. Table {table["name"]}: [!n]')
                if tbl_status == MigrateStatus.CREATED:
                    logger.info('created')
                    m_entry = MigrationEntry(table['name'],
                                             ResourceKind.TABLE,
                                             [project_name])
                    migration_rollback_manager.push_entry(m_entry)
                elif tbl_status == MigrateStatus.SKIPPED:
                    logger.info('skipped creation (was found)')
