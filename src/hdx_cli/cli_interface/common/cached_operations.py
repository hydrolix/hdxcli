from ...library_api.common.generic_resource import access_resource
from ...library_api.common.logging import get_logger

# from ...library_api.utility.decorators import find_in_disk_cache
# from ...library_api.common.config_constants import HDX_CONFIG_DIR
from ...models import ProfileUserContext

logger = get_logger()


def find_kafka(user_ctx: ProfileUserContext) -> list[dict]:
    return access_resource(
        user_ctx,
        [
            ("projects", user_ctx.projectname),
            ("tables", user_ctx.tablename),
            ("sources/kafka", None),
        ],
    )


def find_kinesis(user_ctx: ProfileUserContext) -> list[dict]:
    return access_resource(
        user_ctx,
        [
            ("projects", user_ctx.projectname),
            ("tables", user_ctx.tablename),
            ("sources/kinesis", None),
        ],
    )


def find_siem(user_ctx: ProfileUserContext) -> list[dict]:
    return access_resource(
        user_ctx,
        [
            ("projects", user_ctx.projectname),
            ("tables", user_ctx.tablename),
            ("sources/siem", None),
        ],
    )


def find_projects(user_ctx: ProfileUserContext) -> list[dict]:
    return access_resource(user_ctx, [("projects", None)])


def find_tables(user_ctx: ProfileUserContext) -> list[dict]:
    return access_resource(user_ctx, [("projects", user_ctx.projectname), ("tables", None)])


def find_dictionaries(user_ctx: ProfileUserContext) -> list[dict]:
    return access_resource(user_ctx, [("projects", user_ctx.projectname), ("dictionaries", None)])


def find_functions(user_ctx: ProfileUserContext) -> list[dict]:
    return access_resource(user_ctx, [("projects", user_ctx.projectname), ("functions", None)])


def find_batch_jobs(user_ctx: ProfileUserContext) -> list[dict]:
    return access_resource(user_ctx, [("jobs/batch", None)])


def find_alter_jobs(user_ctx: ProfileUserContext) -> list[dict]:
    alter_jobs = access_resource(user_ctx, [("jobs/alter", None)])
    # Workaround for paginated alter jobs when pagination parameters didn't exist.
    if isinstance(alter_jobs, dict) and alter_jobs.get("results") is not None:
        return alter_jobs.get("results")
    return alter_jobs


def find_transforms(user_ctx: ProfileUserContext) -> list[dict]:
    return access_resource(
        user_ctx,
        [("projects", user_ctx.projectname), ("tables", user_ctx.tablename), ("transforms", None)],
    )


def find_views(user_ctx: ProfileUserContext) -> list[dict]:
    return access_resource(
        user_ctx,
        [("projects", user_ctx.projectname), ("tables", user_ctx.tablename), ("views", None)],
    )


def find_storages(user_ctx: ProfileUserContext) -> list[dict]:
    return access_resource(user_ctx, [("storages", None)])


def find_pools(user_ctx: ProfileUserContext) -> list[dict]:
    return access_resource(user_ctx, [("pools", None)], base_path="/config/v1/")


def find_credentials(user_ctx: ProfileUserContext) -> list[dict]:
    return access_resource(user_ctx, [("credentials", None)])


def find_users(user_ctx: ProfileUserContext) -> list[dict]:
    return access_resource(user_ctx, [("users", None)], base_path="/config/v1/")


def find_invites_user(user_ctx: ProfileUserContext) -> list[dict]:
    return access_resource(user_ctx, [("invites", None)], base_path="/config/v1/")


def find_roles(user_ctx: ProfileUserContext) -> list[dict]:
    return access_resource(user_ctx, [("roles", None)], base_path="/config/v1/")


def find_service_accounts(user_ctx: ProfileUserContext) -> list[dict]:
    return access_resource(user_ctx, [("service_accounts", None)], base_path="/config/v1/")


# @find_in_disk_cache(cache_file=HDX_CONFIG_DIR / "cache/cache.bin", namespace="projects_ids")
# def find_project_id(user_ctx: ProfileUserContext, project_name: str) -> list[str]:
#     projects = find_projects(user_ctx)
#     return [t["uuid"] for t in projects if t["name"] == project_name]
#
#
# @find_in_disk_cache(cache_file=HDX_CONFIG_DIR / "cache/cache.bin", namespace="tables_ids")
# def find_table_id(user_ctx: ProfileUserContext, table_name: str) -> list[str]:
#     tables = find_tables(user_ctx)
#     return [t["uuid"] for t in tables if t["name"] == table_name]
#
#
# @find_in_disk_cache(cache_file=HDX_CONFIG_DIR / "cache/cache.bin", namespace="transforms_ids")
# def find_transform_id(user_ctx, transform_name):
#     transforms = find_transforms(user_ctx)
#     return [t["uuid"] for t in transforms if t["name"] == transform_name]
