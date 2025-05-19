from functools import partial

import click

from ...library_api.common.exceptions import ResourceNotFoundException
from ...library_api.common.generic_resource import access_resource
from ...library_api.common.logging import get_logger
from ...library_api.utility.decorators import (
    dynamic_confirmation_prompt,
    ensure_logged_in,
    report_error_and_exit,
)
from ...library_api.utility.file_handling import load_json_settings_file
from ...models import ProfileUserContext
from ..common.misc_operations import settings_with_force as command_settings_with_force
from ..common.rest_operations import list_ as command_list
from ..common.rest_operations import show as command_show
from ..common.undecorated_click_commands import basic_create, basic_delete

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


@click.group(help="Storage-related operations")
@click.option(
    "--storage",
    "storage_name",
    metavar="STORAGENAME",
    default=None,
    help="Perform operation on the passed storage.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
@ensure_logged_in
def storage(ctx: click.Context, storage_name: str):
    user_profile = ctx.parent.obj["usercontext"]
    org_id = user_profile.org_id
    ctx.obj = {"resource_path": f"/config/v1/orgs/{org_id}/storages/", "usercontext": user_profile}
    ProfileUserContext.update_context(user_profile, storagename=storage_name)


@click.command(
    help="Create storage. You can either specify a settings file using"
    "the -f or --settings-filename option, or provide the storage "
    "configuration directly using the -p, -n, -r, and -c options."
)
@click.argument("storage_name")
@click.option(
    "-f",
    "--settings-filename",
    "settings_file",
    type=click.Path(exists=True, readable=True),
    default=None,
    required=False,
    callback=load_json_settings_file,
    help="Filename containing storage configuration settings.",
)
@click.option(
    "-p", "--bucket-path", default=None, required=False, help="Path to the storage bucket."
)
@click.option(
    "-n", "--bucket-name", default=None, required=False, help="Name of the storage bucket."
)
@click.option("-r", "--region", default=None, required=False, help="Region for the storage bucket.")
@click.option(
    "-c", "--cloud", default=None, required=False, help="Type of cloud storage (e.g., aws, gcp)."
)
@click.option(
    "-E", "--endpoint", default=None, required=False, help="Endpoint for the storage bucket."
)
@click.option(
    "-C",
    "--credential-name",
    "credential_id",
    default=None,
    required=False,
    callback=get_credential_id,
    help="Name of the credential to use for the storage bucket.",
)
@click.option(
    "-M",
    "--io-perf-mode",
    default=None,
    required=False,
    type=click.Choice(["aggressive", "moderate", "relaxed"]),
    help="I/O performance mode for the storage bucket.",
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
def create(
    ctx: click.Context,
    storage_name: str,
    settings_file: dict,
    bucket_path: str,
    bucket_name: str,
    region: str,
    cloud: str,
    endpoint: str,
    credential_id: str,
    io_perf_mode: str,
):
    if not settings_file and not all((bucket_path, bucket_name, region, cloud)):
        raise click.BadParameter(
            "You must specify either a settings file or the bucket path, name, region, and cloud."
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

    basic_create(user_profile, resource_path, storage_name, body=body)
    logger.info(f"Created storage {storage_name}")


_confirmation_prompt = partial(
    dynamic_confirmation_prompt,
    prompt="Please type 'delete this resource' to delete: ",
    confirmation_message="delete this resource",
    fail_message="Incorrect prompt input: resource was not deleted",
)


@click.command(help="Delete resource.")
@click.option(
    "--disable-confirmation-prompt",
    is_flag=True,
    show_default=True,
    default=False,
    help="Suppress confirmation to delete resource.",
)
@click.argument("resource_name")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def delete(ctx: click.Context, resource_name: str, disable_confirmation_prompt: bool):
    _confirmation_prompt(prompt_active=not disable_confirmation_prompt)
    resource_path = ctx.parent.obj.get("resource_path")
    user_profile = ctx.parent.obj.get("usercontext")
    if not basic_delete(user_profile, resource_path, resource_name, force_operation=True):
        logger.info(f"Could not delete {resource_name}. Not found")
        return
    logger.info(f"Deleted {resource_name}")


storage.add_command(command_list)
storage.add_command(create)
storage.add_command(delete)
storage.add_command(command_show)
storage.add_command(command_settings_with_force, name="settings")
