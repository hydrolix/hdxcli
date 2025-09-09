import click
from InquirerPy import inquirer
from rich.columns import Columns
from rich.console import Console
from rich.table import Table

from hdx_cli.cli_interface.common.click_extensions import HdxGroup, HdxCommand
from hdx_cli.cli_interface.common.undecorated_click_commands import basic_create
from hdx_cli.cli_interface.common.misc_operations import settings as command_settings
from hdx_cli.cli_interface.common.rest_operations import delete as command_delete
from hdx_cli.cli_interface.common.rest_operations import list_ as command_list
from hdx_cli.cli_interface.common.rest_operations import show as command_show
from hdx_cli.library_api.common.generic_resource import access_resource
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import report_error_and_exit, ensure_logged_in
from hdx_cli.models import ProfileUserContext

logger = get_logger()
console = Console()


@click.group(cls=HdxGroup)
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
    """Provides commands to create, list, show, and delete credentials.
    It also includes a command to list all available credential types
    which is useful before creating a new one."""
    user_profile = ctx.parent.obj["usercontext"]
    ProfileUserContext.update_context(user_profile, credentialname=credential_name)
    org_id = user_profile.org_id
    ctx.obj = {
        "resource_path": f"/config/v1/orgs/{org_id}/credentials/",
        "usercontext": user_profile,
    }


@click.command(cls=HdxCommand)
@click.argument("resource_name")
@click.argument("credential_type")
@click.option("--description", required=False, help="Credential description.")
@click.option(
    "--detail",
     "details",
    required=False,
    default=None,
    nargs=2,
    multiple=True,
    help="A key-value pair for a credential detail. Use multiple times for multiple details.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(
    ctx: click.Context,
    resource_name: str,
    credential_type: str,
    description: str,
    details: tuple,
):
    """Create a new {resource}.
    The command prompts for any required details not provided as options.
    For fully non-interactive use, all details must be specified
    using the `--detail` option.

    \b
    Examples:
      # Create a {resource} interactively
      {full_command_prefix} create {example_name} gcp-service-account

    \b
      # Create a {resource} non-interactively with key-value details
      {full_command_prefix} create aws-prod-keys aws_access_keys --detail access_key_id "your-id" --detail secret_access_key "your-secret"
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

    details = dict(details) if details else {}

    try:
        # Prompt for any missing required fields
        for field, props in required_fields.items():
            if field not in details and props.get("required"):
                details[field] = inquirer.text(
                    message=f"Enter value for '{field}':",
                    validate=lambda val: bool(val),
                    invalid_message=f"'{field}' is required. Please provide a value.",
                ).execute()
    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user.")
        return

    body = {
        "description": description,
        "type": credential_type,
        "details": details,
    }
    basic_create(profile, resource_path, resource_name, body=body)
    logger.info(f"Created {ctx.parent.command.name} '{resource_name}'")


@click.command(cls=HdxCommand, name="list-types")
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
    """List available {resource} types.

    \b
    Examples:
      # List all available {resource} types, filtering by 'azure' cloud
      {full_command_prefix} list-types --cloud azure
    """
    profile = ctx.parent.obj["usercontext"]
    credential_types = access_resource(profile, [("credentials/types", None)])

    cloud = cloud.lower() if cloud else None

    table = Table(show_header=True, box=None, padding=(0, 1), header_style="bold", pad_edge=False)
    table.add_column("Name", style="dim", no_wrap=True)
    table.add_column("Cloud")
    table.add_column("Required Parameters")

    for cred, info_cred in credential_types.items():
        cloud_name = info_cred.get("cloud", "unknown")
        cloud_name = cloud_name.lower() if cloud_name else None
        if cloud and cloud_name != cloud:
            continue

        parameters = info_cred.get("fields", {})
        required_params = [name for name, details in parameters.items() if details.get("required")]

        if not required_params:
            table.add_row(cred, cloud_name, "[dim]No required parameters.[/dim]")
            continue

        permission_renderable = Columns(required_params, equal=True, column_first=True)
        table.add_row(cred, cloud_name, permission_renderable)

    if not table.rows:
        logger.info(f"No credential types found for cloud '{cloud}'.")
        return
    console.print(table)


credential.add_command(command_list)
credential.add_command(create)
credential.add_command(command_delete)
credential.add_command(command_show)
credential.add_command(command_settings)
credential.add_command(list_types)
