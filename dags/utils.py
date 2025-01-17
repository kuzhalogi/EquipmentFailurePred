import os
import json
import requests
import logging

config_file_path = '/home/kuzhalogi/WorkSpace/EquipmentFailurePred/dags/criticality_config.json'
TEAMS_WEBHOOK_URL = "https://epitafr.webhook.office.com/webhookb2/42d835c5-6eba-462d-8503-6cc1eb47b5de@3534b3d7-316c-4bc9-9ede-605c860f49d2/IncomingWebhook/82e48edd72164ab89088ef43b68a825c/d347d272-aa92-4186-b646-b83c5739ffe9/V2mwqEiG62A_icb_P65wyiggrIsLpefcSturPhBkyBCEs1"

# Load criticality types from the JSON file
def load_criticality_config():
    if os.path.exists(config_file_path):
        with open(config_file_path, 'r') as file:
            criticality_config = json.load(file)
        return criticality_config
    else:
        raise FileNotFoundError(f"Criticality configuration file not found at {config_file_path}")


# Get the criticality level for an expectation
def get_criticality(expectation_type, column_name, criticality_config):
    expectation = f"{expectation_type}:{column_name}"
    
    if expectation in criticality_config["high"]:
        return "high"
    elif expectation in criticality_config["medium"]:
        return "medium"
    elif expectation in criticality_config["low"]:
        return "low"
    else:
        return "unknown"

# Function to send alerts to Teams (or other services)
def send_teams_alert(message: str):
    # Teams webhook URL
      # Replace with your webhook URL
    payload = {"text": message}
    headers = {'Content-Type': 'application/json'}
    response = requests.post(TEAMS_WEBHOOK_URL, data=json.dumps(payload), headers=headers)

    if response.status_code == 200:
        logging.info("Alert sent successfully to Teams.")
    else:
        logging.error(f"Failed to send alert to Teams. Status code: {response.status_code}")




