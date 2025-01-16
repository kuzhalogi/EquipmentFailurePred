import json
import requests
import logging

# Load criticality types from the JSON file
def load_criticality_config():
    with open('criticality_config.json', 'r') as file:
        return json.load(file)

criticality_config = load_criticality_config()

# Get the criticality level for an expectation
def get_criticality(expectation_type, column_name, criticality_config):
    expectation = f"{expectation_type}:{column_name}"
    
    if expectation in criticality_config["high_criticality"]:
        return "high"
    elif expectation in criticality_config["medium_criticality"]:
        return "medium"
    elif expectation in criticality_config["low_criticality"]:
        return "low"
    else:
        return "unknown"

# Function to send alerts to Teams (or other services)
def send_teams_alert(message: str):
    # Teams webhook URL
    teams_webhook_url = "https://outlook.office.com/webhook/..."  # Replace with your webhook URL
    payload = {"text": message}
    headers = {'Content-Type': 'application/json'}
    response = requests.post(teams_webhook_url, data=json.dumps(payload), headers=headers)

    if response.status_code == 200:
        logging.info("Alert sent successfully to Teams.")
    else:
        logging.error(f"Failed to send alert to Teams. Status code: {response.status_code}")




