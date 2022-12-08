from dataclasses import dataclass
from enum import Enum
from typing import List
from urllib.parse import urlparse


from ..common.undecorated_click_commands import basic_delete
from ...library_api.common.generic_resource import access_resource_detailed
from ...library_api.common.context import ProfileUserContext


class MigrateStatus(Enum):
    CREATED = 0
    SKIPPED = 1


class ResourceKind(Enum):
    PROJECT = 0
    TABLE = 1
    TRANSFORM = 2
    FUNCTION = 3
    DICTIONARY = 4

@dataclass(frozen=True)
class MigrationEntry:
    name: str
    resource_kind: str = ResourceKind.PROJECT
    parents: List[str] = None


class MigrationRollbackManager:
    def __init__(self, profile):
        self._profile: ProfileUserContext = profile
        self.migration_entries_stack: List[MigrationEntry] = []

    def push_entry(self, entry: MigrationEntry):
        self.migration_entries_stack.append(entry)

    def pop_entry(self):
        if len(self.migration_entries_stack) != 0:
            return self.migration_entries_stack.pop(-1)
        return None

    def _rollback_entry(self, entry: MigrationEntry):
        """Rollback entry. Does not check for migrate_status and does it unconditionally"""
        if entry.resource_kind == ResourceKind.PROJECT:
            _, project_url = access_resource_detailed(self._profile, 
                                     [('projects', entry.name)])
            split_path = urlparse(project_url).path.split('/')
            resource_path = '/'.join(split_path[:-2])
            if basic_delete(self._profile, resource_path, entry.name):
                print(f'Rolled back project {entry.name}')
        elif entry.resource_kind == ResourceKind.FUNCTION:
            _, function_url = access_resource_detailed(self._profile,
                                                    [('projects', entry.parents[0]),
                                                     ('functions', entry.name)])
            split_path = urlparse(function_url).path.split('/')
            resource_path = '/'.join(split_path[:-2])
            if basic_delete(self._profile, resource_path, entry.name):
                print(f'Rolled back function {entry.name}')

        elif entry.resource_kind == ResourceKind.DICTIONARY:
            _, dict_url = access_resource_detailed(self._profile,
                                                    [('projects', entry.parents[0]),
                                                     ('dictionaries', entry.name)])
            split_path = urlparse(dict_url).path.split('/')
            resource_path = '/'.join(split_path[:-2])
            if basic_delete(self._profile, resource_path, entry.name):
                print(f'Rolled back dictionary {entry.name}')
        elif entry.resource_kind == ResourceKind.TABLE:
            _, table_url = access_resource_detailed(self._profile,
                                                    [('projects', entry.parents[0]),
                                                     ('tables', entry.name)])
            split_path = urlparse(table_url).path.split('/')
            resource_path = '/'.join(split_path[:-2])
            if basic_delete(self._profile, resource_path, entry.name):
                print(f'Rolled back table {entry.name}')
        elif entry.resource_kind == ResourceKind.TRANSFORM:
            _, transform_url = access_resource_detailed(self._profile,
                                                        [('projects', entry.parents[0]),
                                                         ('tables', entry.parents[1]),
                                                         ('transforms', entry.name)])
            split_path = urlparse(transform_url).path.split('/')
            resource_path = '/'.join(split_path[:-2])
            if basic_delete(self._profile, resource_path, entry.name):
                print(f'Rolled back transform {entry.name}')

    def _rollback(self):
        current_entry = self.pop_entry()
        while current_entry:
            self._rollback_entry(current_entry)
            current_entry = self.pop_entry()

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):

        if not traceback:
            return
        print('Rolling back migration changes...')
        done = False
        while not done:
            try:
                self._rollback()
                done = True
            except KeyboardInterrupt:
                result = input('A rollback was in progress, are you sure you want to abort without rolling back? (y/n): ')
                done = result.lower() == 'y'


class DoNothingMigrationRollbackManager:
    def __init__(self, profile):
        pass

    def push_entry(self, entry: MigrationEntry):
        pass

    def pop_entry(self):
        pass

    def _rollback_entry(self, entry: MigrationEntry):
        pass

    def _rollback(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        pass
