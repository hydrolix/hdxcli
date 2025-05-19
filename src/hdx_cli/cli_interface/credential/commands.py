import json

import click

from ...library_api.common.generic_resource import access_resource
from ...library_api.common.logging import get_logger
from ...library_api.utility.decorators import ensure_logged_in, report_error_and_exit
from ...models import ProfileUserContext
from ..common.misc_operations import settings as command_settings
from ..common.rest_operations import delete as command_delete
from ..common.rest_operations import list_ as command_list
from ..common.rest_operations import show as command_show
from ..common.undecorated_click_commands import basic_create

logger = get_logger()


@click.group(help="Credential-related operations")
@click.option(
    "--credential",
    "credential_name",
    metavar="CREDENTIAL_NAME",
    default=None,
    help="Perform operation on the passed credential.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def credential(ctx: click.Context, credential_name: str):
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(user_profile, credentialname=credential_name)
    org_id = user_profile.org_id
    ctx.obj = {
        "resource_path": f"/config/v1/orgs/{org_id}/credentials/",
        "usercontext": user_profile,
    }


@click.command(help="Create a new credential.")
@click.argument("credential_name", metavar="CREDENTIAL_NAME")
@click.argument("credential_type", metavar="CREDENTIAL_TYPE")
@click.option("--description", required=False, help="Credential description.")
@click.option(
    "--details",
    required=False,
    default=None,
    help='Credential details as a JSON string (e.g., \'{"key1": "value1", "key2": "value2"}\').',
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(
    ctx: click.Context, credential_name: str, credential_type: str, description: str, details: str
):
    """
    Create a new credential by providing all parameters in a single command
    or by providing them interactively.
    """
    profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")

    # Retrieve available credential types
    credential_types = access_resource(profile, [("credentials/types", None)])
    if credential_type not in credential_types:
        raise click.BadParameter(
            f"'{credential_type}' is not a valid credential type. "
            f"Consider using the 'list-types' command."
        )

    credential_type_info = credential_types[credential_type]
    required_fields = credential_type_info.get("fields", {})

    # Parse JSON string for --details if provided
    if details:
        try:
            details = json.loads(details)
        except json.JSONDecodeError as exc:
            raise click.BadParameter(
                "Invalid format for --details. It should be a valid JSON string."
            ) from exc
    else:
        details = {}

    # Prompt for any missing required fields
    for field, props in required_fields.items():
        if field not in details:
            attempts = 0
            while attempts < 3:
                logger.info(
                    f"Enter value for '{field}' "
                    f"({'Required' if props['required'] else 'Optional'}): [!i]"
                )
                value = input().strip()
                if not value and props.get("required"):
                    logger.info(f"'{field}' is required. Please provide a value.")
                    attempts += 1
                elif value:
                    details[field] = value
                    break
                else:
                    break
            else:
                logger.info("Too many failed attempts. Canceling operation.")
                return

    body = {
        "description": description,
        "type": credential_type,
        "details": details,
    }
    basic_create(profile, resource_path, credential_name, body=body)
    logger.info(f"Created credential {credential_name}")


@click.command(help="List credential types.")
@click.option(
    "--cloud",
    "-c",
    "cloud",
    metavar="CLOUD",
    required=False,
    default=None,
    help="Filter the credential types by a specific cloud.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def list_types(ctx: click.Context, cloud: str):
    profile = ctx.parent.obj["usercontext"]
    credential_types = access_resource(profile, [("credentials/types", None)])

    cloud = cloud.lower() if cloud else None
    for cred, info_cred in credential_types.items():
        cloud_name = info_cred.get("cloud", "unknown")
        cloud_name = cloud_name.lower() if cloud_name else None
        if cloud and cloud_name != cloud:
            continue

        logger.info(f"\nName: {cred}\nCloud: {cloud_name}")
        parameters = info_cred.get("fields", {})
        param_lines = [
            f"  - {name} ({'required' if details.get('required') else 'optional'})"
            for name, details in sorted(
                parameters.items(), key=lambda x: not x[1].get("required", False)
            )
        ]

        if param_lines:
            logger.info("\n".join(param_lines))


credential.add_command(command_list)
credential.add_command(create)
credential.add_command(command_delete)
credential.add_command(command_show)
credential.add_command(command_settings)
credential.add_command(list_types, name="list-types")
