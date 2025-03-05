# --------------------
# Import Statements
# --------------------
from dotenv import load_dotenv
import os
import pandas as pd
from app.assets.extract_transform import extract_stock_data, transform_data
from app.connectors.db_connector import get_engine, load_data

# --------------------
# Load Environment Variables
# --------------------
load_dotenv()

api_key = os.getenv('API_KEY')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_server_name = os.getenv('DB_SERVER_NAME')
db_database_name = os.getenv('DB_DATABASE_NAME')

if not all([api_key, db_user, db_password, db_server_name, db_database_name]):
    raise ValueError("One or more required environment variables are missing.")

# --------------------
# Create Database Connection
# --------------------
engine = get_engine(db_user, db_password, db_server_name, db_database_name)

# --------------------
# Run ETL Pipeline
# --------------------
if __name__ == "__main__":
    # Extract Data
    df = extract_stock_data(api_key)

    # Transform Data
    df_stocks_selected = transform_data(df)

    # Load Data into Database
    load_data(df_stocks_selected, engine)
