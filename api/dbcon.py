from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy import text
from fastapi import HTTPException

COLM_ORDER = ['Product ID', 'Air temperature [K]', 'Process temperature [K]', 'Rotational speed [rpm]', 'Torque [Nm]', 'Tool wear [min]', 'Type']

FETCH_ORDER = COLM_ORDER + ['date','source']

# postgres Database
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "postgres"


DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
PREDICTION_TABLE = 'prediction'
FAILURE_MODE_TABLE = 'failure_modes'
DATABASE_ENGINE = create_engine(DATABASE_URL)


def insert_predictions(df):
    """Insert predictions into the database."""
    try:
        df.to_sql(PREDICTION_TABLE, DATABASE_ENGINE, if_exists='append', index=False)
        print(f"Successfully inserted {len(df)} rows into {PREDICTION_TABLE}.")
    except Exception as e:
        print(f"Error inserting predictions: {e}")    


def insert_failure_modes(df):
    """Insert failure mode records into the database safely and efficiently."""
    try:
        df.to_sql(FAILURE_MODE_TABLE, DATABASE_ENGINE, if_exists="append", index=False, method="multi")

        print(f"Successfully inserted {len(df)} rows into {FAILURE_MODE_TABLE}.")
    except Exception as e:
        print(f"Error inserting failure modes: {e}")


def fetch_past_predictions(from_date, to_date, source):
    """Fetch past predictions from the database."""
    query = f"""
        SELECT * 
        FROM prediction
        WHERE date >= '{from_date}' 
        AND date <= '{to_date}'
        AND source = '{source}';
    """
    return pd.read_sql(query, DATABASE_ENGINE)