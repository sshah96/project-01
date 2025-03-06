# --------------------
# Import Statements
# --------------------
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from sqlalchemy.engine import URL


# Import custom modules from assets
from assets.extract_transform import extract_stock_data, transform_data, load_data
from connectors.db_connector import get_engine, load_data
# --------------------
# Main Execution Block
# --------------------
if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv()

    # Accessing the environment variables
    api_key = os.getenv('API_KEY')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_server_name = os.getenv('DB_SERVER_NAME')
    db_database_name = os.getenv('DB_DATABASE_NAME')

    # Check if all required environment variables are set
    if not api_key or not db_user or not db_password or not db_server_name or not db_database_name:
        raise ValueError("One or more required environment variables are missing.")
    
    # Create the engine with pg8000
    connection_url = URL.create(
        drivername="postgresql+pg8000",  # Using pg8000 instead of psycopg2
        username=db_user,
        password=db_password,
        host=db_server_name, 
        port=5432,
        database=db_database_name
    )
    engine = create_engine(connection_url)

    # --------------------
    # Extract Data
    # --------------------
    df = extract_stock_data(api_key=api_key)
    
    # --------------------
    # Transform Data
    # --------------------
    df_stocks_selected = transform_data(df)
    
    # --------------------
    # Load Data into the Database
    # --------------------
    load_data(df_stocks_selected, engine)
