import os
import json
import logging
import requests
import pandas as pd
from datetime import datetime
import great_expectations as gx


DATA_FEED_FOLDER = "/home/kuzhalogi/WorkSpace/EquipmentFailurePred/Data_Feed"
RAW_DATA = os.path.join(DATA_FEED_FOLDER, "raw-data")
GOOD_DATA_FOLDER = os.path.join(DATA_FEED_FOLDER, "good-data")
BAD_DATA_FOLDER = os.path.join(DATA_FEED_FOLDER, "bad-data")
PROCESSED_FILE='/home/kuzhalogi/WorkSpace/EquipmentFailurePred/Data_Feed/processed_files.txt'
GREAT_EXPECTATION = '/home/kuzhalogi/WorkSpace/EquipmentFailurePred/gx'
SUITE_NAME = "milling_machine_data_quality"
TEAMS_WEBHOOK_URL = "https://epitafr.webhook.office.com/webhookb2/42d835c5-6eba-462d-8503-6cc1eb47b5de@3534b3d7-316c-4bc9-9ede-605c860f49d2/IncomingWebhook/82e48edd72164ab89088ef43b68a825c/d347d272-aa92-4186-b646-b83c5739ffe9/V2mwqEiG62A_icb_P65wyiggrIsLpefcSturPhBkyBCEs1"
API_PREDICT_ENDPOINT = 'http://localhost:8000/predict'


def load_validation_suite(fil_path: str):
    context = gx.get_context(context_root_dir=GREAT_EXPECTATION)
    suite = context.get_expectation_suite(SUITE_NAME)
    gx_df = gx.read_csv(fil_path)
    return gx_df, suite


def validation_result_to_df(validation_result):
    results = []
    
    for result in validation_result.results:
        row = {
            "expectation_type": result.expectation_config.expectation_type,
            "success": result.success,
            "column": result.expectation_config.kwargs.get("column"),
        }
        results.append(row)
    
    df = pd.DataFrame(results)
    return df


def validate_rows(df: pd.DataFrame, suite: any) -> list:

    all_validation_failed = []
    for i, row in df.iterrows():
        # Convert the row into a single-row DataFrame
        row_df = row.to_frame().T
        
        # Convert the DataFrame to a Great Expectations DataFrame
        gx_df = gx.from_pandas(row_df)
        
        # Validate the row against the expectation suite
        resu = gx_df.validate(expectation_suite=suite)
        
        # Convert the validation result to a DataFrame
        validation_r = validation_result_to_df(resu)
        
        validation_r["row_index"] = i # adding row's index
        
        # filter only failed validations
        failed_validations = validation_r[validation_r["success"] == False]
        
        all_validation_failed.append(failed_validations)
        
    return all_validation_failed


def get_validation_stats(OG_df: pd.DataFrame,failed_validations_df: pd.DataFrame):

    total_rows = len(OG_df)
    # Number of rows with at least one failed expectation
    bad_rows = failed_validations_df[failed_validations_df["success"] == False]["row_index"].nunique()
    # Number of rows with all expectations passed
    good_rows = total_rows - bad_rows
    # Failure rate (percentage of rows with at least one failed expectation)
    failure_rate = (bad_rows / total_rows) * 100
    stats_capsule = {"total_rows":total_rows,
                     "good_rows":good_rows,
                     "bad_rows":bad_rows,
                     "failure_rate":failure_rate,
                     }
    return stats_capsule


def get_error_statistics(statistics):
    total_rows_count = statistics["total_rows"]
    valid_rows_count = statistics["valid_rows"]
    invalid_rows_count = statistics["invalid_rows"]
    file_name = statistics["file_name"]
    error_rate = round(statistics["error_rate"],2)
    return total_rows_count, valid_rows_count, invalid_rows_count, file_name, error_rate
    

def get_failed_stats(failed_validations_df: pd.DataFrame):
    # Most common failed expectations
    most_common_failed_expectations = (
        failed_validations_df[failed_validations_df["success"] == False]["expectation_type"]
        .value_counts()
        .reset_index()
    )
    most_common_failed_expectations.columns = ["expectation_type", "failure_count"]
    # Columns with the most failures
    columns_with_most_failures = (
        failed_validations_df[failed_validations_df["success"] == False]["column"]
        .value_counts()
        .reset_index()
    )
    columns_with_most_failures.columns = ["column", "failure_count"]
    
    return most_common_failed_expectations, columns_with_most_failures


def get_criticality(error_rate):
    if error_rate >= 50:
        criticality = "High"
    elif 20 <= error_rate < 50:
        criticality = "Medium"
    else:
        criticality = "Low"
    return criticality


def build_alert_message(capsule, criticality):
    """
    Build an alert message from validation results.
    """
    file_name=capsule["stats"]["file_name"]
    error_rate = capsule["stats"]["error_rate"]
    processed_at = capsule["stats"]["processed_at"]
    alert_message = (
        f"Data validation failed. Issues detected:\n"
        f"- Filename: {file_name}\n"
        f"- Criticality: {criticality}\n"
        f"- Error Rate: {error_rate}\n"
        f"- Processed at: {processed_at}\n"
    )
    
    return alert_message


def send_teams_alert(message: str):
    payload = {"text": message}
    headers = {'Content-Type': 'application/json'}
    response = requests.post(TEAMS_WEBHOOK_URL, data=json.dumps(payload), headers=headers)

    if response.status_code == 200:
        logging.info("Alert sent successfully to Teams.")
    else:
        logging.error(f"Failed to send alert to Teams. Status code: {response.status_code}")
        
        
def load_dataframe(file_path):
    """Load a CSV file into a DataFrame."""
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        logging.error(f"Failed to load CSV file {file_path}: {e}")
        return None


def split_dataframe(df, failed_validations_df):
    """Split DataFrame into good and bad rows based on validation results."""
    row_with_errors = set(failed_validations_df["row_index"].unique())
    error_indices_list = list(row_with_errors)

    good_rows = df.drop(error_indices_list)
    bad_rows = df.loc[error_indices_list]

    return good_rows, bad_rows


def save_dataframe(df, folder, file_name):
    """Save a DataFrame to a CSV file in the specified folder."""
    file_path = os.path.join(folder, file_name)
    try:
        df.to_csv(file_path, index=False)
        logging.info(f"File saved: {file_path}")
    except Exception as e:
        logging.error(f"Failed to save file {file_path}: {e}")
    return file_path


def delete_file(file_path):
    """Delete a file and log the result."""
    try:
        os.remove(file_path)
        logging.info(f"Deleted file: {file_path}")
    except Exception as e:
        logging.error(f"Failed to delete {file_path}: {e}")
        
        
def to_str(df)-> str:
    json_data = df.to_json(orient='records')
    json_string = json.dumps(json_data)
    return json_string


def call_to_prediction_api(file_name,df):
    response = requests.post(
            API_PREDICT_ENDPOINT,
            json={
                "source": 'scheduler',
                "df":df}
        )
    return response


def predict_new_data(file_name):
    file_path = os.path.join(GOOD_DATA_FOLDER, file_name)
    data = pd.read_csv(file_path)
    df = pd.DataFrame(data)
    df_str = to_str(df)
    response = call_to_prediction_api(file_name, df=df_str)
    
    if response.status_code == 200:
        processed_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')   
        with open(PROCESSED_FILE, 'a') as f:
            f.write(f"{file_name}\t{processed_at}\n") # Writing data in TSV format
        result = True
    else:
        result = False
    
    return result