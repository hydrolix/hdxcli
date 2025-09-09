import click

from hdx_cli.cli_interface.common.click_extensions import HdxGroup, HdxCommand
from hdx_cli.cli_interface.common.misc_operations import settings as command_settings
from hdx_cli.cli_interface.common.rest_operations import list_ as command_list
from hdx_cli.cli_interface.common.rest_operations import show as command_show
from hdx_cli.cli_interface.common.rest_operations import delete as command_delete
from hdx_cli.cli_interface.common.undecorated_click_commands import basic_create
from hdx_cli.library_api.common.exceptions import ResourceNotFoundException
from hdx_cli.library_api.common.generic_resource import access_resource
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.utility.decorators import report_error_and_exit, ensure_logged_in
from hdx_cli.library_api.utility.file_handling import read_json_from_file
from hdx_cli.models import ProfileUserContext

logger = get_logger()


@report_error_and_exit(exctype=Exception)
def get_credential_id(ctx, param, value):
    if value is None:
        return value

    user_profile = ctx.parent.obj.get("usercontext")
    try:
        credential = access_resource(user_profile, [("credentials", value)])
    except ResourceNotFoundException as exc:
        raise click.BadParameter(f"Credential name '{value}' not found.") from exc
    return credential.get("uuid")


@click.group(cls=HdxGroup)
@click.option(
    "--storage",
    "storage_name",
    metavar="STORAGE_NAME",
    default=None,
    help="Perform operation on the passed storage.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def storage(ctx: click.Context, storage_name: str):
    """This group of commands allows to create, list,
    show, and delete storages."""
    user_profile = ctx.parent.obj["usercontext"]
    org_id = user_profile.org_id
    ctx.obj = {"resource_path": f"/config/v1/orgs/{org_id}/storages/", "usercontext": user_profile}
    ProfileUserContext.update_context(user_profile, storagename=storage_name)


@click.command(cls=HdxCommand)
@click.argument("resource_name", required=True)
@click.option(
    "-f",
    "--settings-filename",
    type=click.Path(exists=True, readable=True),
    default=None,
    help="Path to a JSON file with storage configuration settings.",
)
@click.option(
    "-p",
    "--bucket-path",
    default=None,
    help="Path to the storage bucket.",
)
@click.option(
    "-n",
    "--bucket-name",
    default=None,
    help="Name of the storage bucket.",
)
@click.option(
    "-r",
    "--region",
    default=None,
    help="Region for the storage bucket.",
)
@click.option(
    "-c",
    "--cloud",
    default=None,
    help="Type of cloud storage (e.g., aws, gcp).",
)
@click.option(
    "-E",
    "--endpoint",
    default=None,
    help="Endpoint for the storage bucket.",
)
@click.option(
    "-C",
    "--credential-name",
    "credential_id",
    default=None,
    callback=get_credential_id,
    help="Name of the credential to use for the storage bucket.",
)
@click.option(
    "-M",
    "--io-perf-mode",
    default=None,
    type=click.Choice(["aggressive", "moderate", "moderate"], case_sensitive=False),
    help="I/O performance mode for the storage bucket.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(
    ctx: click.Context,
    resource_name: str,
    settings_filename: str,
    bucket_path: str,
    bucket_name: str,
    region: str,
    cloud: str,
    endpoint: str,
    credential_id: str,
    io_perf_mode: str,
):
    """Create a new {resource}.

    \b
    A {resource} can be created in two ways:
      - Using a JSON settings file via `--settings-filename`.
      - Providing individual settings as options (`--bucket-path`, `--bucket-name`, etc).

    \b
    Examples:
      # Create a {resource} from a settings file
      {full_command_prefix} create {example_name} -f path/to/storage.json

    \b
      # Create a {resource} using individual options
      {full_command_prefix} create gcp-storage --bucket-path my-path --bucket-name my-bucket --region us-central1 --cloud gcp
    """
    settings_file = read_json_from_file(settings_filename) if settings_filename else None
    if not settings_filename and not all((bucket_path, bucket_name, region, cloud)):
        raise click.BadParameter(
            "Either --settings-filename or all required individual options "
            "(--bucket-path, --bucket-name, --region, --cloud) must be provided."
        )

    user_profile = ctx.parent.obj.get("usercontext")
    resource_path = ctx.parent.obj.get("resource_path")

    if not settings_file:
        storage_settings = {
            "bucket_path": bucket_path,
            "bucket_name": bucket_name,
            "region": region,
            "cloud": cloud,
            "endpoint": endpoint,
            "credential_id": credential_id,
            "io_perf_mode": io_perf_mode,
        }
        body = {
            "settings": {
                key: value for key, value in storage_settings.items() if value is not None
            },
        }
    else:
        body = settings_file

    basic_create(user_profile, resource_path, resource_name, body=body)
    logger.info(f"Created {ctx.parent.command.name} {resource_name}")


storage.add_command(command_list)
storage.add_command(create)
storage.add_command(command_show)
storage.add_command(command_delete)
storage.add_command(command_settings)
