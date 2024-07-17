from queue import Queue


def recovery_process(files_to_migrate: list, migrated_files: list,
                     migrated_files_queue: Queue) -> list[tuple[str, int]]:
    """
    The objective of this method is to remove from 'files_to_migrate' list
    the files that have already been migrated and are already in the bucket,
    comparing with 'migrated_files' list.

    :param files_to_migrate: List of tuples, each containing (file_path, data_size)
                             representing files to migrate
    :param migrated_files: List of root paths of files that were already migrated
    :param migrated_files_queue: Queue of files migrated during the current run
    :return: A new list of file paths to migrate, excluding those that have already been migrated
    """
    new_file_paths = []
    for path, data_size in files_to_migrate:
        _, _, data_path = path.partition('/data/v2/current/')
        if data_path in migrated_files:
            migrated_files_queue.put((path, data_size))
        else:
            new_file_paths.append((path, data_size))

    return new_file_paths
