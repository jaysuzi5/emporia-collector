import yaml
from pathlib import Path
from typing import Any, Optional, Union

class Config:
    """
    Configuration loader for YAML files with support for nested keys and normalized lists of dicts.

    Usage:
        config = Config()

        print(config.get("logging_info.component"))  # 'emporia-collector'
        print(config.get("servers.1"))               # 'worker2'
        print(config.get("people.john.first_name"))  # 'John'
        print(config.get("people.jane.last_name"))   # 'John'
    """

    def __init__(self, path: str = "src/configuration/config.yaml"):
        self.path = Path(path)
        if not self.path.exists():
            raise FileNotFoundError(f"Config file not found: {self.path}")
        self._load()

    def _load(self):
        """Load YAML and normalize lists of single-key dicts into dicts."""
        with open(self.path, "r") as f:
            self._config = yaml.safe_load(f) or {}
        self._normalize(self._config)

    def _normalize(self, obj):
        """Recursively convert lists of single-key dicts into dicts keyed by that key."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                obj[k] = self._normalize(v)
        elif isinstance(obj, list):
            # If every item is a dict with exactly 1 key, convert to a dict
            if all(isinstance(i, dict) and len(i) == 1 for i in obj):
                new_dict = {}
                for i in obj:
                    key = list(i.keys())[0]
                    new_dict[key] = self._normalize(i[key])
                return new_dict
            else:
                obj = [self._normalize(i) for i in obj]
        return obj

    def reload(self):
        """Reload config from disk."""
        self._load()

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        """
        Retrieve a value using dot notation:
        - Supports dicts and normalized lists of dicts
        - Example: 'people.john.first_name' -> 'John'
        """
        parts = key.split(".")
        current: Union[dict, list, Any] = self._config

        for part in parts:
            if isinstance(current, dict):
                if part in current:
                    current = current[part]
                else:
                    if default is not None:
                        return default
                    raise KeyError(f"Key not found: {key}")
            elif isinstance(current, list):
                try:
                    idx = int(part)
                    current = current[idx]
                except (ValueError, IndexError):
                    if default is not None:
                        return default
                    raise KeyError(f"Invalid list index '{part}' in key: {key}")
            else:
                if default is not None:
                    return default
                raise KeyError(f"Cannot traverse key '{part}' in '{key}', got {type(current).__name__}")
        return current
