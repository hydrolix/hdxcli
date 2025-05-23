import json

import click

from ....library_api.common.exceptions import ResourceNotFoundException
from ....library_api.common.logging import get_logger
from ....library_api.utility.decorators import report_error_and_exit
from ....library_api.utility.file_handling import load_json_settings_file
from ....models import ProfileUserContext
from ...common.cached_operations import find_transforms
from ...common.misc_operations import settings as command_settings
from ...common.rest_operations import delete as command_delete
from ...common.rest_operations import list_ as command_list
from ...common.rest_operations import show as command_show
from ...common.undecorated_click_commands import basic_create, basic_show

logger = get_logger()


@click.group(help="Batch Job-related operations")
@click.option(
    "--project",
    "project_name",
    metavar="PROJECTNAME",
    default=None,
    help="Use or override project set in the profile.",
)
@click.option(
    "--table",
    "table_name",
    metavar="TABLENAME",
    default=None,
    help="Use or override table set in the profile.",
)
@click.option(
    "--transform",
    "transform_name",
    metavar="TRANSFORMNAME",
    default=None,
    help="Explicitly pass the transform name. If none is given, "
    "the default transform for the used table is used.",
)
@click.option(
    "--job",
    "batch_name",
    metavar="JOBNAME",
    default=None,
    help="Perform operation on the passed job name.",
)
@click.pass_context
def batch(ctx: click.Context, project_name, table_name, transform_name, batch_name):
    user_profile = ctx.parent.obj["usercontext"]
    batch_path = f'{ctx.parent.obj["resource_path"]}batch/'
    ctx.obj = {"resource_path": batch_path, "usercontext": user_profile}
    ProfileUserContext.update_context(
        user_profile,
        projectname=project_name,
        tablename=table_name,
        transformname=transform_name,
        batchname=batch_name,
    )


batch.add_command(command_delete)
batch.add_command(command_list)
batch.add_command(command_show)
batch.add_command(command_settings)


# pylint:disable=line-too-long
@batch.command(
    help="Ingest data into a table. The data path url can be local or point to a bucket. "
    "Your cluster (and not your client machine executing hdxcli tool) *must have* "
    "permissions to access the data bucket in case you need to ingest directly from there. "
    "If the data path is a directory, the directory data will be used."
)
@click.argument("jobname")
@click.argument(
    "jobname_file",
    metavar="JOBNAMEFILE",
    type=click.Path(exists=True, readable=True),
    callback=load_json_settings_file,
)
@click.pass_context
@report_error_and_exit(exctype=Exception)
# pylint:enable=line-too-long
def ingest(ctx: click.Context, jobname: str, jobname_file: dict):
    resource_path = ctx.parent.obj["resource_path"]
    user_profile = ctx.parent.obj["usercontext"]
    if not user_profile.projectname or not user_profile.tablename:
        raise ResourceNotFoundException(
            f"No project/table parameters provided and "
            f"no project/table set in profile '{user_profile.profilename}'"
        )

    body = jobname_file or {}
    transform_name = user_profile.transformname
    if not transform_name:
        transforms_list = find_transforms(user_profile)
        try:
            transform_name = [t["name"] for t in transforms_list if t["settings"]["is_default"]][0]
        except (IndexError, KeyError) as exc:
            raise ResourceNotFoundException(
                "No default transform found to apply ingest command and " "no --transform passed."
            ) from exc
    body["settings"]["source"]["table"] = f"{user_profile.projectname}.{user_profile.tablename}"
    body["settings"]["source"]["transform"] = transform_name
    basic_create(user_profile, resource_path, jobname, body=body)
    logger.info(f"Started job {jobname}")


@batch.command(help="Cancel a batch job.")
@click.argument("job_name")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def cancel(ctx: click.Context, job_name):
    resource_path = ctx.parent.obj["resource_path"]
    user_profile = ctx.parent.obj["usercontext"]
    batch_job_id = json.loads(basic_show(user_profile, resource_path, job_name)).get("uuid")
    cancel_job_path = f"{resource_path}{batch_job_id}/cancel"
    basic_create(user_profile, cancel_job_path)
    logger.info(f"Cancelled batch job {job_name}")


@batch.command(help="Retry a failed batch job.")
@click.argument("job_name")
@click.pass_context
@report_error_and_exit(exctype=Exception)
def retry(ctx, job_name):
    resource_path = ctx.parent.obj["resource_path"]
    user_profile = ctx.parent.obj["usercontext"]
    batch_job_id = json.loads(basic_show(user_profile, resource_path, job_name)).get("uuid")
    retry_job_path = f"{resource_path}{batch_job_id}/retry"
    basic_create(user_profile, retry_job_path)
    logger.info(f"Retried batch job {job_name}")
