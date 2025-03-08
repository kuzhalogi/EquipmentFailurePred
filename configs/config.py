import os
import yaml
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env
load_dotenv()

# Load the config.yaml file
config_path = Path(__file__).parent / "config.yaml"
with open(config_path, "r") as file:
    config = yaml.safe_load(file)

# Replace YAML values with environment variables if they exist
for key, value in config.items():
    if isinstance(value, dict):  # Nested dict
        for sub_key, sub_value in value.items():
            if isinstance(sub_value, str) and sub_value.startswith("${"):
                env_var = sub_value.strip("${}")
                config[key][sub_key] = os.getenv(env_var, sub_value)
    elif isinstance(value, str) and value.startswith("${"):
        env_var = value.strip("${}")
        config[key] = os.getenv(env_var, value)

# Expose the config as a module-level variable
def get_config():
    return config
