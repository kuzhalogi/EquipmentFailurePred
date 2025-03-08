import yaml
import os

CONFIG_PATH = os.getenv("MODEL_CONFIG_PATH", None)

if CONFIG_PATH and os.path.exists(CONFIG_PATH):
    print(f"Using external config file: {CONFIG_PATH}")
else:
    CONFIG_PATH = os.path.join(os.path.dirname(__file__), "model_config.yaml")
    print(f"Using default package config: {CONFIG_PATH}")

# Load YAML config
def load_config(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)

config = load_config(CONFIG_PATH)


# config = load_config('model_config.yaml')
features = config["features"]
model = config["model"]

NUMERICAL = features["numerical"]
ORDINAL = features["ordinal"]
FEATURES = features["only_features_columns"]
TARGET = features["target"]

MODEL_PATH = model["model_path"]
SCALER_PATH = model["scaler_path"]
LENCODER_PATH = model["lencoder_path"]