from sqlalchemy import create_engine

COLM_ORDER = ['Product ID', 'Air temperature [K]', 'Process temperature [K]', 'Rotational speed [rpm]', 'Torque [Nm]', 'Tool wear [min]', 'Type']

FETCH_ORDER = COLM_ORDER + ['date','source']

# Database 1
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "my_newdb"

# Database 2
# DB_USER = "yingdisu"
# DB_PASSWORD = ""
# DB_HOST = "localhost"
# DB_PORT = "9999"
# DB_NAME = "yingdisu"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
TABLE = 'prediction'
engine = create_engine(DATABASE_URL)
