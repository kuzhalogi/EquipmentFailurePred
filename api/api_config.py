import yaml
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="../.env", override=True)

def load_config(path):
    """Load YAML config and replace environment variables."""
    with open(path, "r") as f:
        config = yaml.safe_load(f)

    def resolve_env_vars(value):
        """Replace ${VAR_NAME} in YAML with actual environment values."""
        if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
            env_var_name = value[2:-1]  # Extract variable name
            return os.getenv(env_var_name, f"⚠️ MISSING: {env_var_name}")  # Default warning if missing
        elif isinstance(value, dict):
            return {k: resolve_env_vars(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [resolve_env_vars(v) for v in value]
        return value

    return resolve_env_vars(config)

config = load_config('api_config.yaml')

db = config["database"]
DATABASE_URL = f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['name']}"
PREDICTION_TABLE = db["prediction_table_name"]
FAILURE_MODE_TABLE = db["failure_mode_table_name"]
COLUMN_ORDER = config["features"]["column_order"]