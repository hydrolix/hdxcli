"""Transform resource command. It flows down url for current table"""

import json
from typing import Optional

import click

from ...library_api.common.exceptions import CommandLineException
from ...library_api.common.logging import get_logger
from ...library_api.ddl.common_algo import (
    ddl_datatype_to_hdx_datatype,
    ddl_to_create_table_info,
    generate_transform_dict,
)
from ...library_api.ddl.common_intermediate_representation import DdlCreateTableInfo
from ...library_api.utility.decorators import (
    ensure_logged_in,
    force_operation_option,
    no_rollback_option,
    report_error_and_exit,
    skip_group_logic_on_help,
    target_cluster_options,
)
from ...library_api.utility.file_handling import load_json_settings_file
from ...models import ProfileUserContext
from ..common.migration.resource_migrations import migrate_resource_config
from ..common.misc_operations import settings_with_force as command_settings_with_force
from ..common.rest_operations import delete as command_delete
from ..common.rest_operations import list_ as command_list
from ..common.rest_operations import show as command_show
from ..common.undecorated_click_commands import basic_create, basic_transform

logger = get_logger()


@click.group(help="Transform-related operations")
@click.option(
    "--project",
    "project_name",
    help="Use or override project set in the profile.",
    metavar="PROJECTNAME",
    default=None,
)
@click.option(
    "--table",
    "table_name",
    help="Use or override table set in the profile.",
    metavar="TABLENAME",
    default=None,
)
@click.option(
    "--transform",
    "transform_name",
    help="Explicitly pass the transform name. If none is given, "
    "the default transform for the used table is used.",
    metavar="TRANSFORMNAME",
    default=None,
)
@click.pass_context
@skip_group_logic_on_help
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def transform(ctx: click.Context, project_name: str, table_name: str, transform_name: str):
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(
        user_profile, projectname=project_name, tablename=table_name, transformname=transform_name
    )
    basic_transform(ctx)


@click.command(help="Create transform.")
@click.argument("transform_name", metavar="TRANSFORMNAME", required=True)
@click.option(
    "--body-from-file",
    "-f",
    type=click.Path(exists=True, readable=True),
    help="Path to a file containing settings for the transform."
    "'name' key from the body will be replaced by the given 'resource_name'.",
    metavar="BODYFROMFILE",
    default=None,
    callback=load_json_settings_file,
)
@force_operation_option
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(ctx: click.Context, transform_name: str, body_from_file: dict, force_operation: bool):
    user_profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]
    basic_create(
        user_profile,
        resource_path,
        transform_name,
        body=body_from_file,
        force_operation=force_operation,
    )
    logger.info(f"Created transform {transform_name}")


@click.command(help="Map a transform from a description language. (currently sql CREATE TABLE)")
@click.option(
    "--ddl-mapping",
    help="Mapping repository file to use for ddl to data type translation",
    metavar="DDLMAPPING",
    required=False,
)
@click.option(
    "--ddl-custom-mapping",
    help="Mapping file to use for the ddl data type translation",
    metavar="DDLCUSTOMMAPPING",
    required=False,
)
@click.option(
    "--no-apply",
    is_flag=True,
    default=False,
    help="Just output the transform without applying it into the table",
)
@click.option(
    "--source-mapping-ddl-name",
    "-s",
    help="The language used in the source mapping. For sql it can be autodected "
    "from the the ddl_file extension. Use when disambiguation is needed.",
    metavar="SOURCEMAPPING",
    required=False,
    default=None,
)
@click.option(
    "--user-choices",
    help="Json file with user choices. These are applied to your mapping as a postprocess step",
    metavar="USERCHOICESFILE",
    required=False,
)
@click.argument("ddl_file")
@click.argument("transform_name")
@click.pass_context
@report_error_and_exit(exctype=Exception)
# pylint: disable=too-many-arguments
def map_from(
    ctx: click.Context,
    ddl_file: str,
    ddl_mapping: Optional[str],
    ddl_custom_mapping: Optional[str],
    no_apply: bool,
    source_mapping_ddl_name: Optional[str],
    user_choices: Optional[str],
    transform_name: str,
):
    if source_mapping_ddl_name is None:
        if ddl_file.lower().endswith(".sql"):
            source_mapping_ddl_name = "sql"
    if not source_mapping_ddl_name:
        raise CommandLineException(
            f"Unkown source mapping dsl for {ddl_file}. " "Please specify one with -s option."
        )

    ddl_file_contents = None
    with open(ddl_file, encoding="utf-8") as ddl_stream:
        ddl_file_contents = ddl_stream.read()

    mapper = ddl_datatype_to_hdx_datatype(ddl_custom_mapping, source_mapping_ddl_name)
    create_table_info: DdlCreateTableInfo = ddl_to_create_table_info(
        ddl_file_contents, source_mapping_ddl_name, mapper, user_choices_file=user_choices
    )
    transform_dict = generate_transform_dict(
        create_table_info, transform_name, transform_type=create_table_info.ingest_type
    )
    transform_str = json.dumps(transform_dict)
    if no_apply:
        logger.info(transform_str)
        return

    user_profile = ctx.parent.obj["usercontext"]
    resource_path = ctx.parent.obj["resource_path"]
    basic_create(user_profile, resource_path, transform_name, body=transform_str)
    logger.info(f"Created transform {transform_name}")


@click.command(
    help=(
        "Migrate a transform to a target project and table.\n\n"
        "This command migrates a transform from the source project and table in the current profile "
        "to a specified target project and table in the target profile or cluster.\n\n"
        "Provide a target profile using --target-profile or specify cluster details "
        "(hostname, username, password, and URI scheme)."
    )
)
@click.argument("target_project_name", metavar="TARGET_PROJECT_NAME", required=True, default=None)
@click.argument("target_table_name", metavar="TARGET_TABLE_NAME", required=True, default=None)
@click.argument("new_transform_name", metavar="NEW_TRANSFORM_NAME", required=True, default=None)
@target_cluster_options
@no_rollback_option
@click.pass_context
@report_error_and_exit(exctype=Exception)
def migrate(
    ctx: click.Context,
    target_project_name: str,
    target_table_name: str,
    new_transform_name: str,
    target_profile: str,
    target_cluster_hostname: str,
    target_cluster_username: str,
    target_cluster_password: str,
    target_cluster_uri_scheme: str,
    no_rollback: bool,
):
    source_profile = ctx.parent.obj["usercontext"]

    if not source_profile.transformname:
        raise click.BadParameter("No source transform provided.")
    if target_profile is None and not (
        target_cluster_hostname
        and target_cluster_username
        and target_cluster_password
        and target_cluster_uri_scheme
    ):
        raise click.BadParameter(
            "Either provide a --target-profile or all four target cluster options."
        )

    data = {
        "source_profile": source_profile,
        "target_profile_name": target_profile,
        "target_cluster_hostname": target_cluster_hostname,
        "target_cluster_username": target_cluster_username,
        "target_cluster_password": target_cluster_password,
        "target_cluster_uri_scheme": target_cluster_uri_scheme,
        "source_project": source_profile.projectname,
        "target_project": target_project_name,
        "source_table": source_profile.tablename,
        "target_table": target_table_name,
        "source_transform": source_profile.transformname,
        "target_transform": new_transform_name,
        "no_rollback": no_rollback,
    }
    migrate_resource_config("transform", **data)

    logger.info("All resources migrated successfully")


transform.add_command(map_from)
transform.add_command(create)
transform.add_command(command_delete)
transform.add_command(command_list)
transform.add_command(command_show)
transform.add_command(command_settings_with_force, "settings")
transform.add_command(migrate)
