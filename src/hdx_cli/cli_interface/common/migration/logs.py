from enum import Enum, auto
from typing import Union

from ....library_api.common.logging import get_logger
from .migration_rollback import (
    MigrateStatus,
    MigrationEntry,
    MigrationRollbackManager,
    ResourceKind,
)

logger = get_logger()


class LogType(Enum):
    INFO = auto()
    SUCCESS = auto()
    WARNING = auto()
    PROMPT = auto()


def log_message(log_type: LogType, message: str, indent: int = 2):
    """
    Prints a message with a specified log type and indentation.

    :param log_type: An enum value (INFO, SUCCESS, WARNING, PROMPT, etc.)
    :param message: The text to display.
    :param indent: Number of spaces to indent the message.
    """
    log_type_label = {
        LogType.INFO: "INFO",
        LogType.SUCCESS: "SUCCESS",
        LogType.WARNING: "WARNING",
        LogType.PROMPT: "PROMPT",
    }.get(log_type, "INFO")

    indent_spaces = " " * indent
    logger.info(f"{indent_spaces}[{log_type_label}] {message}")


def log_without_type(messages: Union[str, list[str]], indent: int = 2, sub_indent: int = 4):
    """
    Prints one or more lines without a log type label, applying consistent indentation.

    :param messages:   A string or list of strings to display.
    :param indent:     Number of spaces to indent before the 'sub_indent' is applied.
    :param sub_indent: Additional indentation for each line.
    """
    base_spaces = " " * (indent + sub_indent)

    if isinstance(messages, list):
        for msg in messages:
            logger.info(f"{base_spaces}{msg}")
    else:
        logger.info(f"{base_spaces}{messages}")


def log_migration_status(
    resource_type: str,
    resource_name: str,
    status: MigrateStatus,
    rollback_manager: MigrationRollbackManager,
    resource_kind: ResourceKind,
    parents: list[str] = None,
):
    """
    Logs the migration status of a resource.

    Args:
        resource_type (str): The type of resource being migrated.
        resource_name (str): The name of the resource.
        status (MigrateStatus): The status of the migration.
        rollback_manager (MigrationRollbackManager): The rollback manager instance.
        resource_kind (ResourceKind): The kind of the resource.
        parents (list[str]): The list of parent resources, if any.
    """
    result = "None"
    if status == MigrateStatus.CREATED:
        result = "Migrated"
        m_entry = MigrationEntry(resource_name, resource_kind, parents)
        rollback_manager.push_entry(m_entry)
    elif status == MigrateStatus.SKIPPED:
        result = "Skipped (was found)"
    message = f"{resource_type} '{resource_name}' {result}"
    log_message(LogType.SUCCESS, message, indent=0)
