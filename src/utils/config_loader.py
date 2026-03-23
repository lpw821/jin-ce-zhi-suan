# src/utils/config_loader.py
import json
import os

class ConfigLoader:
    _instance = None
    _config = {}

    def __new__(cls, config_path="config.json"):
        if cls._instance is None:
            cls._instance = super(ConfigLoader, cls).__new__(cls)
            cls._instance.load_config(config_path)
        return cls._instance

    def load_config(self, config_path):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(base_dir))
        base_config_path = config_path if os.path.exists(config_path) else os.path.join(project_root, "config.json")
        base_config = self._load_json_config(base_config_path)
        private_config_path = os.environ.get("CONFIG_PRIVATE_PATH", os.path.join(project_root, "config.private.json"))
        private_config = self._load_json_config(private_config_path, silent=True)
        self._config = self._deep_merge_dict(base_config, private_config)

    def _load_json_config(self, config_path, silent=False):
        import re
        if not os.path.exists(config_path):
            if not silent:
                print(f"Config file not found: {config_path}")
            return {}
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                content = f.read()
                pattern = r'("[^"]*")|(\/\/.*)'
                def replace(match):
                    if match.group(1):
                        return match.group(1)
                    return ""
                content = re.sub(pattern, replace, content)
                return json.loads(content)
        except Exception as e:
            if not silent:
                print(f"Error loading config: {e}")
            return {}

    def _deep_merge_dict(self, base, override):
        if not isinstance(base, dict):
            return override if override is not None else base
        if not isinstance(override, dict):
            return base
        merged = dict(base)
        for k, v in override.items():
            if isinstance(v, dict) and isinstance(merged.get(k), dict):
                merged[k] = self._deep_merge_dict(merged[k], v)
            else:
                merged[k] = v
        return merged

    def get(self, key, default=None):
        keys = key.split('.')
        value = self._config
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default

    @classmethod
    def reload(cls, config_path="config.json"):
        """Force reload the config from disk"""
        cls._instance = None
        return cls(config_path)

    def set(self, key, value):
        """
        Set a config value by dot notation key (e.g. "data_provider.source")
        """
        keys = key.split('.')
        current = self._config
        for k in keys[:-1]:
            current = current.setdefault(k, {})
        current[keys[-1]] = value

    def to_dict(self):
        return json.loads(json.dumps(self._config, ensure_ascii=False))

    def save(self, config_path="config.json"):
        if not os.path.exists(config_path):
            base_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(os.path.dirname(base_dir))
            config_path = os.path.join(project_root, "config.json")
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(self._config, f, ensure_ascii=False, indent=2)
