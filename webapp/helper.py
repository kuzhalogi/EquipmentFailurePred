from io import StringIO
import streamlit as st
import pandas as pd
import numpy as np
import requests
import json


ID = ['Product ID']
FEATURES = ['Air temperature [K]', 'Process temperature [K]', 'Rotational speed [rpm]', 'Torque [Nm]', 'Tool wear [min]', 'Type']
COLNAME = ID + FEATURES
MODEL_API_URL = "http://127.0.0.1:8000/predict"
DB_API_URL = "http://127.0.0.1:8000/past-predictions"


def to_str(df)-> str:
    json_data = df.to_json(orient='records')
    json_string = json.dumps(json_data)
    return json_string

    
def to_ar(json_str: str):
    json_list = json.loads(json_str)
    arr = np.array(json_list)
    return arr


def to_df(json_string: str)-> pd.DataFrame:
    json_data = json.loads(json_string)
    json_data_io = StringIO(json_data)
    df = pd.read_json(json_data_io)
    return df


def prepare_value_for_df(result):
    try:
        result_list = json.loads(result)
        if isinstance(result_list, list):
            final_value = [int(x) for x in result_list]
        else:
            final_value = int(result_list)
    except json.JSONDecodeError:
        if isinstance(result, str) and result.isdigit():
            final_value = int(result)
        else:
            final_value = None
    except ValueError:
        final_value = None
    return final_value



def add_prediction_column_to_df(df, prediction_result):
    final_value = prepare_value_for_df(prediction_result)
    df['Predictions'] = final_value
    return df


def callmodel(df):
    df_str = to_str(df)
    response = requests.post(MODEL_API_URL, json={"source": 'webapp', "df":df_str})
    if response.status_code == 200:
        result = response.json()['pred']
        st.success("Done!")
        return result
    else:
        st.error("Failed to make prediction. Please try again.")
        return 'Failed!'