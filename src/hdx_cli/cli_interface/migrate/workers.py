import threading
import time
from queue import Queue

from hdx_cli.library_api.provider import BaseProvider


class CountingQueue(Queue):
    def __init__(self, elements: list = None):
        super().__init__()
        if elements is not None:
            for element in elements:
                self.put(element)


class ReaderWorker(threading.Thread):
    def __init__(self, reader_queue: Queue, writer_queue: Queue, exceptions: Queue,
                 provider: BaseProvider, workers_amount: int, target_project_id=None,
                 target_table_id=None):
        threading.Thread.__init__(self)
        self.reader_queue = reader_queue
        self.writer_queue = writer_queue
        self.exceptions = exceptions
        self.provider = provider
        self.workers_amount = workers_amount
        self.target_project_id = target_project_id
        self.target_table_id = target_table_id

    def run(self):
        try:
            while True:
                # If there are exceptions or the work queue is empty, it stops.
                if not self.exceptions.empty() or self.reader_queue.empty():
                    break

                # Queue for writers is too full
                # Preserve memory space
                if self.writer_queue.qsize() > (self.workers_amount * 2):
                    time.sleep(1)
                    continue

                path, data_size = self.reader_queue.get()
                data = self.provider.read_file(path)

                # Migrating partitions into a different root_path (project/table)
                if self.target_project_id and self.target_table_id:
                    split_path = path.split('/')
                    split_path[2] = self.target_project_id
                    split_path[3] = self.target_table_id
                    path = "/".join(split_path)

                self.writer_queue.put((path, data, data_size))
        except Exception as exc:
            self.exceptions.put(exc)


class WriterWorker(threading.Thread):
    def __init__(self, writer_queue: Queue, migrated_files_queue: Queue, exceptions: Queue,
                 provider: BaseProvider):
        threading.Thread.__init__(self)
        self.writer_queue = writer_queue
        self.migrated_files_queue = migrated_files_queue
        self.exceptions = exceptions
        self.provider = provider

    def run(self):
        try:
            while True:
                if not self.exceptions.empty():
                    return

                item = self.writer_queue.get()
                if item is None:
                    return
                path, data, data_size = item

                self.provider.write_file(path, data)
                self.migrated_files_queue.put((path, data_size))
                self.writer_queue.task_done()
        except Exception as exc:
            self.exceptions.put(exc)
