import sqlalchemy as sa
from datetime import datetime
import logging

DATABASE_CONN_STR = 'postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/postgres'

def get_db_connection():
    """
    Create and return a database connection.
    """
    engine = sa.create_engine(DATABASE_CONN_STR)
    connection = engine.connect()
    metadata = sa.MetaData()
    return engine, connection, metadata

def save_validation_statistics(file_name, total_rows, good_rows, bad_rows, failure_rate):
    """
    Save validation statistics to the validation_statistics table.
    """
    try:
        engine, connection, metadata = get_db_connection()
        validation_stats_table = sa.Table('validation_statistics', metadata, autoload_with=engine)

        stats_data = {
            "file_name": file_name,
            "total_rows": total_rows,
            "good_rows": good_rows,
            "bad_rows": bad_rows,
            "failure_rate": failure_rate,
            "processed_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        # Use a transaction to ensure atomicity
        with connection.begin():
            connection.execute(validation_stats_table.insert().values(stats_data))
        
        logging.info(f"Validation statistics saved for file: {file_name}")
    except Exception as e:
        logging.error(f"Failed to save validation statistics: {e}")
        raise  # Re-raise the exception to propagate the error
    finally:
        connection.close()

def save_failed_expectations(file_name, failed_expectations):
    """
    Save failed expectations to the failed_expectations table.
    """
    try:
        engine, connection, metadata = get_db_connection()
        failed_expectations_table = sa.Table('failed_expectations', metadata, autoload_with=engine)

        # Prepare data for bulk insert
        data = [
            {
                "file_name": file_name,
                "expectation_type": row["expectation_type"],
                "failure_count": row["failure_count"]
            }
            for _, row in failed_expectations.iterrows()
        ]

        # Use a transaction to ensure atomicity
        with connection.begin():
            if data:  # Only insert if there is data
                connection.execute(failed_expectations_table.insert(), data)
        
        logging.info(f"Saved {len(data)} failed expectations for file: {file_name}")
    except Exception as e:
        logging.error(f"Failed to save failed expectations for file {file_name}: {e}")
        raise  # Re-raise the exception to propagate the error
    finally:
        connection.close()

def save_failed_columns(file_name, failed_columns):
    """
    Save columns with the most failures to the failed_columns table.
    """
    try:
        engine, connection, metadata = get_db_connection()
        failed_columns_table = sa.Table('failed_columns', metadata, autoload_with=engine)

        # Prepare data for bulk insert
        data = [
            {
                "file_name": file_name,
                "column_name": row["column"],
                "failure_count": row["failure_count"]
            }
            for _, row in failed_columns.iterrows()
        ]

        # Use a transaction to ensure atomicity
        with connection.begin():
            if data:  # Only insert if there is data
                connection.execute(failed_columns_table.insert(), data)
        
        logging.info(f"Saved {len(data)} failed columns for file: {file_name}")
    except Exception as e:
        logging.error(f"Failed to save failed columns for file {file_name}: {e}")
        raise  # Re-raise the exception to propagate the error
    finally:
        connection.close()