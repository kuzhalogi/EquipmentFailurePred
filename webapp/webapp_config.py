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


config = load_config('webapp_config.yaml')

# ID = ['Product ID']
# FEATURES = ['Air temperature [K]', 'Process temperature [K]', 'Rotational speed [rpm]', 'Torque [Nm]', 'Tool wear [min]', 'Type']
# COLNAME = ID + FEATURES
# MODEL_API_URL = "http://127.0.0.1:8000/predict"
# DB_API_URL = "http://127.0.0.1:8000/past-predictions"


ID = config["features"]["id"]
FEATURES = config["features"]["only_features_columns"]
COLUMN_ORDER = config["features"]["column_order"]
MODEL_API_ENDPOINT = config["api"]["predict_endpoint"]
DB_API_ENDPOINT = config["api"]["past_predictions_endpoint"]
