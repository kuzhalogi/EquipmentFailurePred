import yaml
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="/home/kuzhalogi/WorkSpace/EquipmentFailurePred/.env", override=True)

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


config = load_config('/home/kuzhalogi/WorkSpace/EquipmentFailurePred/dag_config.yaml')

db = config["database"]
DATA_FEED_FOLDER = config["data"]["data_feed_folder"]
RAW_DATA_FOLDER = config["data"]["raw_data_folder"]
GOOD_DATA_FOLDER = config["data"]["good_data_folder"]
BAD_DATA_FOLDER = config["data"]["bad_data_folder"]
PROCESSED_FILE= config["data"]["processed_file"]
GREAT_EXPECTATION = config["monitoring"]["great_expectation"]
SUITE_NAME = config["monitoring"]["suite_name"]
TEAMS_WEBHOOK_URL = config["webhook"]["teams_url"]
MODEL_API_ENDPOINT = config["api"]["predict_endpoint"]
DATABASE_CONN_STR = f"{db['driver']}://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['name']}"