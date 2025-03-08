import json
import datetime
import numpy as np
import pandas as pd
from io import StringIO
from api_config import *


# COLM_ORDER = [
#     "Product ID", "Air temperature [K]", "Process temperature [K]", 
#     "Rotational speed [rpm]", "Torque [Nm]", "Tool wear [min]", 
#     "Type"
# ]


def ar_tostr(data):
    data_list = data.tolist()
    json_string = json.dumps(data_list)
    return json_string


def to_df(json_string: str)-> pd.DataFrame:
    json_data = json.loads(json_string)
    json_data_io = StringIO(json_data)
    df = pd.read_json(json_data_io)
    return df


def to_str(df)-> str:
    json_data = df.to_json(orient='records')
    json_string = json.dumps(json_data)
    return json_string

    
def to_ar(json_str: str):
    json_list = json.loads(json_str)
    arr = np.array(json_list)
    return arr


def format_predictions(df, predictions, probabilities, source):
    """Format predictions DataFrame before inserting into the database."""
    df = df[COLUMN_ORDER].copy()
    df['Predictions'] = predictions
    df['failure_probability'] = probabilities[:, 1]  
    current_date = datetime.datetime.now()
    df['date'] = current_date.strftime("%Y-%m-%d %H:%M:%S") 
    df['source'] = source

    return df.rename(columns={
        'Product ID': 'product_id',
        'Air temperature [K]': 'air_temperature_k',
        'Process temperature [K]': 'process_temperature_k',
        'Rotational speed [rpm]': 'rotational_speed_rpm',
        'Torque [Nm]': 'torque_nm',
        'Tool wear [min]': 'tool_wear_min',
        'Type': 'type',
        'Predictions': 'prediction'
    })


def detect_failure_modes(df):
    """Detect failure modes and return a DataFrame with flags for each product_id."""
    
    failure_data = []
    
    for _, row in df.iterrows():
        product_id = row["product_id"]

        # Initialize failure flags
        failure_flags = {"twf": False, "hdf": False, "pwf": False, "osf": False}

        # Tool Wear Failure (twf)
        if 200 <= row["tool_wear_min"] <= 240:
            failure_flags["twf"] = True

        # Heat Dissipation Failure (hdf)
        if (row["process_temperature_k"] - row["air_temperature_k"] < 8.6) and row["rotational_speed_rpm"] < 1380:
            failure_flags["hdf"] = True

        # Power Failure (pwf)
        power = row["torque_nm"] * row["rotational_speed_rpm"]
        if power < 3500 or power > 9000:
            failure_flags["pwf"] = True

        # Overstrain Failure (osf)
        variant_thresholds = {"L": 11000, "M": 12000, "H": 13000}
        product_variant = row["type"]
        if product_variant in variant_thresholds and (row["tool_wear_min"] * row["torque_nm"] > variant_thresholds[product_variant]):
            failure_flags["osf"] = True

        # Append row with product_id and failure flags
        failure_data.append({"product_id": product_id, **failure_flags})

    return pd.DataFrame(failure_data)
