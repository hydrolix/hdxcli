"""Customer resolution and membership handling for migrations.

Since Hydrolix v6.3 (HDX-11681), project creation requires an existing
Customer on the target cluster, and tables may only reference storages and
credentials that are members of the project's customer (HDX-11643). This
module resolves which customer a migrated project should belong to and
registers the storages/credentials the migrated table references.
"""

import json
from urllib.parse import urlparse

from hdx_cli.library_api.common.exceptions import (
    ActionNotAvailableException,
    HdxCliException,
    HttpException,
)
from hdx_cli.library_api.common.generic_resource import access_resource_detailed
from hdx_cli.library_api.common.logging import get_logger
from hdx_cli.library_api.common.storage import get_storage_default
from hdx_cli.models import ProfileUserContext

from ..common.undecorated_click_commands import basic_create, basic_get, basic_options
from .helpers import confirm_action

logger = get_logger()

CUSTOMERS_PATH = "/config/v1/customers/"
# Customers seeded by init_ci; never a sensible default for a migrated project.
CI_CUSTOMER_NAMES = ("hydro", "sample_project")
PICKER_ATTEMPTS = 3


def customer_field_status(profile: ProfileUserContext, projects_path: str) -> tuple[bool, bool]:
    """Whether the target's project endpoint supports and requires a customer.

    Read from the OPTIONS metadata of the projects endpoint, so version
    differences are handled without hardcoded version checks:

    - ``supported`` is False on pre-6.1 clusters (no 'customer' field at all).
    - ``required`` is True only where the server rejects a project created
      without a customer (6.4+). Clusters where the field is present but
      optional (6.1-6.3.x auto-assign the default customer) report False, so
      migrations to them keep working untouched.

    Both are False when the metadata cannot be fetched.
    """
    try:
        structure = basic_options(profile, projects_path)
    except (HttpException, ActionNotAvailableException) as exc:
        logger.debug(f"Could not fetch project options from target: {exc}")
        return False, False
    field = structure.get("customer")
    if field is None:
        return False, False
    return True, bool(field.get("required"))


def get_target_customer(
    profile: ProfileUserContext,
    projects_path: str,
    customer_name_or_uuid: str | None = None,
    existing_project: dict | None = None,
) -> dict | None:
    """Resolve the Customer for the migrated project on the target cluster.

    Returns the customer body (with 'uuid' and 'name'), or None when no
    customer applies (the target does not support them, or the field is
    optional and none was requested so the server assigns the default).
    """
    if existing_project is not None:
        return _customer_of_existing_project(profile, existing_project, customer_name_or_uuid)

    supported, required = customer_field_status(profile, projects_path)
    if not supported:
        # Pre-6.1 target: no customer concept at all.
        if customer_name_or_uuid:
            logger.debug(
                "The target cluster does not support customers on projects, "
                "ignoring --target-customer."
            )
        return None

    # The field exists. Honor an explicit choice on any such cluster (it is
    # settable whether or not it is required).
    if customer_name_or_uuid:
        return _resolve_or_create_from_option(
            profile, list_customers(profile), customer_name_or_uuid
        )

    # No explicit choice: only prompt when the server would otherwise reject
    # the project. Optional-field clusters (6.1-6.3.x) auto-assign the default.
    if not required:
        return None
    return _interactive_pick_customer(profile, list_customers(profile))


def list_customers(profile: ProfileUserContext) -> list[dict]:
    try:
        return basic_get(profile, CUSTOMERS_PATH, pagination=False) or []
    except HttpException as exc:
        raise HdxCliException(f"Unable to list customers on the target cluster: {exc}") from exc


def find_customer(customers: list[dict], name_or_uuid: str) -> dict | None:
    # The server slugifies customer names on create (spaces become dashes), so
    # match the value as typed and in its slugified form. This lets a name with
    # a space still resolve to the customer the server actually stored.
    candidates = {name_or_uuid, name_or_uuid.replace(" ", "-")}
    for customer in customers:
        if candidates & {customer.get("uuid"), customer.get("name")}:
            return customer
    return None


def default_customer_candidate(customers: list[dict]) -> dict | None:
    """The obvious pick: the single customer that is not seeded by init_ci."""
    non_ci = [c for c in customers if c.get("name") not in CI_CUSTOMER_NAMES]
    return non_ci[0] if len(non_ci) == 1 else None


def _customer_of_existing_project(
    profile: ProfileUserContext, existing_project: dict, customer_name_or_uuid: str | None
) -> dict | None:
    customer_id = existing_project.get("customer")
    if not customer_id:
        # Pre-6.3 target, or a legacy project without a customer.
        return None

    customer = find_customer(list_customers(profile), customer_id)
    if not customer:
        customer = {"uuid": customer_id, "name": customer_id}
    if customer_name_or_uuid and customer_name_or_uuid not in (
        customer.get("uuid"),
        customer.get("name"),
    ):
        logger.info(
            f"Warning: the target project already belongs to customer "
            f"'{customer.get('name')}' and this cannot be changed; "
            "ignoring --target-customer."
        )
    return customer


def _resolve_or_create_from_option(
    profile: ProfileUserContext, customers: list[dict], customer_name_or_uuid: str
) -> dict:
    customer = find_customer(customers, customer_name_or_uuid)
    if customer:
        return customer

    available = ", ".join(sorted(c.get("name", "") for c in customers)) or "none"
    logger.info("")
    logger.info(f"Customer '{customer_name_or_uuid}' was not found on the target cluster.")
    logger.info(f"Available customers: {available}.")
    if not confirm_action(f"Create customer '{customer_name_or_uuid}' on the target cluster?"):
        raise HdxCliException(
            f"Customer '{customer_name_or_uuid}' does not exist on the target cluster. "
            "Rerun with --target-customer set to one of the available customers, "
            "or ask an administrator to create it."
        )
    return _create_customer(profile, customer_name_or_uuid)


def _interactive_pick_customer(profile: ProfileUserContext, customers: list[dict]) -> dict:
    logger.info("In progress")
    logger.info("")
    header = " Customer Settings "
    logger.info(f"{header:*^40}")
    logger.info("* The target cluster requires projects to belong to a customer.")
    if customers:
        logger.info("* Available customers on the target cluster:")
        for customer in customers:
            logger.info(f"*   {customer.get('name')} ({customer.get('uuid')})")
    else:
        logger.info("* There are no customers on the target cluster yet.")
    logger.info("*")

    default_customer = default_customer_candidate(customers)
    for _ in range(PICKER_ATTEMPTS):
        if default_customer:
            logger.info(f"* Customer name or UUID ({default_customer.get('name')}): [!i]")
        else:
            logger.info("* Customer name or UUID (created if it does not exist): [!i]")
        user_input = input().strip()

        if not user_input:
            if not default_customer:
                logger.info("* A customer is required. Please try again.")
                continue
            selection = default_customer
        else:
            selection = find_customer(customers, user_input)

        if selection:
            if confirm_action(
                f"* Assign customer '{selection.get('name')}' to the migrated project?"
            ):
                logger.info(f'{"*" * 40:<42} -> [!n]')
                return selection
            continue

        if confirm_action(f"* Customer '{user_input}' does not exist. Create it?"):
            created = _create_customer(profile, user_input)
            logger.info(f'{"*" * 40:<42} -> [!n]')
            return created

    raise HdxCliException(
        "Attempt limit reached. No customer was selected for the migrated project. "
        "Rerun with --target-customer to provide one directly."
    )


def _create_customer(profile: ProfileUserContext, customer_name: str) -> dict:
    try:
        response = basic_create(profile, CUSTOMERS_PATH, customer_name)
    except HttpException as exc:
        if exc.error_code in (401, 403):
            raise HdxCliException(
                f"Insufficient permissions to create customer '{customer_name}' on the "
                "target cluster. Ask an administrator to create it, then rerun the "
                f"migration with --target-customer {customer_name}."
            ) from exc
        raise

    # Use the created resource straight from the POST response: the server may
    # have slugified the name (e.g. spaces to dashes), so reading it back by the
    # typed name would miss it and leave the target already mutated.
    try:
        created = response.json()
    except (ValueError, AttributeError):
        created = {}
    if not created.get("uuid"):
        raise HdxCliException(
            f"Customer '{customer_name}' was created but the response did not " "include its id."
        )
    return {"uuid": created["uuid"], "name": created.get("name", customer_name)}


def collect_storage_map_ids(table_body: dict) -> set[str]:
    storage_map = table_body.get("settings", {}).get("storage_map") or {}
    storage_ids = set()
    if storage_map.get("default_storage_id"):
        storage_ids.add(storage_map["default_storage_id"])
    storage_ids.update((storage_map.get("column_value_mapping") or {}).keys())
    storage_ids.update(storage_map.get("spread_list") or [])
    return storage_ids


def collect_autoingest_credential_ids(table_body: dict) -> set[str]:
    credential_ids = set()
    for entry in table_body.get("settings", {}).get("autoingest") or []:
        if isinstance(entry, dict):
            for key in ("source_credential_id", "bucket_credential_id"):
                if entry.get(key):
                    credential_ids.add(entry[key])
    return credential_ids


def _is_member(resource: dict, customer: dict) -> bool:
    members = resource.get("customers") or []
    return customer.get("uuid") in {m.get("uuid") for m in members if isinstance(m, dict)}


def ensure_storage_memberships(
    profile: ProfileUserContext, customer: dict, table_body: dict, target_storages: list[dict]
) -> str:
    """Register the table's storages with the target customer when needed.

    Tables may only reference storages that are members of the project's
    customer; the cluster default storage is exempt from that validation.
    Returns a short status message for the caller's progress line.
    """
    storage_ids = collect_storage_map_ids(table_body)
    default_storage_id, _ = get_storage_default(target_storages)
    storage_ids.discard(default_storage_id)
    if not storage_ids:
        return "Not needed"

    for storage_id in sorted(storage_ids):
        storage = next((s for s in target_storages if s.get("uuid") == storage_id), None)
        if storage is None:
            # Unknown storage id: let the table creation surface the real error.
            continue
        if "customers" not in storage:
            # This storage does not expose memberships; skip it and let the
            # table creation surface any error rather than abandoning the rest.
            logger.debug(f"Storage '{storage_id}' has no 'customers' field; skipping.")
            continue
        if _is_member(storage, customer):
            continue
        _confirm_and_add_membership(
            profile,
            customer,
            "add_storage",
            {"storages": [{"uuid": storage_id}]},
            f"storage '{storage.get('name')}' ({storage_id})",
            "This makes the storage (and its credential) usable by that customer's users.",
        )
    return "Done"


def ensure_credential_memberships(
    profile: ProfileUserContext, customer: dict, credential_ids: set[str]
) -> None:
    """Register the given credentials (e.g. from autoingest) with the customer."""
    if not credential_ids:
        return

    credentials, _ = access_resource_detailed(profile, [("credentials", None)])
    for credential_id in sorted(credential_ids):
        credential = next((c for c in credentials if c.get("uuid") == credential_id), None)
        if credential is None:
            # Unknown credential id: let the table creation surface the real error.
            continue
        if "customers" not in credential:
            # This credential does not expose memberships; skip it and keep
            # going rather than abandoning the remaining credentials.
            logger.debug(f"Credential '{credential_id}' has no 'customers' field; skipping.")
            continue
        if _is_member(credential, customer):
            continue
        _confirm_and_add_membership(
            profile,
            customer,
            "add_credential",
            {"credentials": [{"uuid": credential_id}]},
            f"credential '{credential.get('name')}' ({credential_id})",
            "This makes the credential usable by that customer's users.",
        )


def _confirm_and_add_membership(
    profile: ProfileUserContext,
    customer: dict,
    action: str,
    body: dict,
    resource_description: str,
    consequence: str,
) -> None:
    customer_name = customer.get("name")
    action_path = f"{CUSTOMERS_PATH}{customer.get('uuid')}/{action}/"
    manual_remediation = (
        f"ask an administrator to run: POST {action_path} "
        f"with body {json.dumps(body)}, then rerun the migration"
    )

    logger.info("")
    logger.info(f"The {resource_description} is not registered with customer '{customer_name}'.")
    logger.info(consequence)
    if not confirm_action(f"Register it with customer '{customer_name}'?"):
        raise HdxCliException(
            f"The {resource_description} must be registered with customer "
            f"'{customer_name}' before the table can be created. To do it manually, "
            f"{manual_remediation}."
        )

    try:
        basic_create(profile, action_path, body=body)
    except HttpException as exc:
        if exc.error_code in (401, 403):
            raise HdxCliException(
                f"Insufficient permissions to register the {resource_description} "
                f"with customer '{customer_name}'. To do it manually, {manual_remediation}."
            ) from exc
        raise


def target_projects_context(profile: ProfileUserContext) -> tuple[dict | None, str]:
    """The existing target project (if any) and the target projects path."""
    projects, projects_url = access_resource_detailed(profile, [("projects", None)])
    projects_path = urlparse(projects_url).path
    existing_project = next((p for p in projects if p.get("name") == profile.projectname), None)
    return existing_project, projects_path
