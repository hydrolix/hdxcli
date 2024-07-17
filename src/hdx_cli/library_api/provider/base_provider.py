from abc import ABC, abstractmethod


class BaseProvider(ABC):
    @abstractmethod
    def setup_connection_to_bucket(self, **kwargs):
        """Set up the connection to the bucket."""
        pass

    @abstractmethod
    def read_file(self, path: str) -> bytes:
        """Reads bytes from files in the bucket."""
        pass

    @abstractmethod
    def write_file(self, path: str, data: bytes):
        """Writes bytes to a bucket."""
        pass

    @abstractmethod
    def list_files_in_path(self, path: str) -> list:
        """Return a list of files from a path."""
        pass
