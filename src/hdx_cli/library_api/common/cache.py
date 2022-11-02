import toml

from .exceptions import LogicException


class CacheDict:
    """A simple cache dictionary"""

    def __init__(self, dict_data, *,
                 _initialized_from_factory=False):
        if not _initialized_from_factory:
            raise LogicException("Must construct CacheDict from factory.")
        self._cache_dict = dict_data

    @classmethod
    def build_from_dict(cls, the_dict: str):
        """Use a dictionary to build a new cache"""
        return cls(the_dict, _initialized_from_factory=True)

    @classmethod
    def build_from_toml_stream(cls, toml_data_stream: str):
        "Use a toml file-like object pointed to by toml_data_stream str"
        the_dict = toml.load(toml_data_stream)
        return cls.build_from_dict(the_dict)

    def save_to_stream(self, stream):
        """Save cache"""
        toml.dump(self._cache_dict, stream)

    def __getitem__(self, item):
        """Get item"""
        return self._cache_dict[item]

    def get(self, key, default=None):
        """Access internal dictionary"""
        return self._cache_dict.get(key, default)

    def has_key(self, key):
        """Check membership of key"""
        return key in self._cache_dict
