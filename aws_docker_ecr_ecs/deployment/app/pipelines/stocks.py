# Library imports
from app.assets.extract import extract_stock_data, transform_data
from app.connectors.db_connector import get_engine, load_data
import os
from dotenv import load_dotenv

# Loading environment variables
load_dotenv()
api_key = os.getenv('API_KEY')

if not api_key:
    raise ValueError(" API_KEY is missing. Please check your .env file.")

# Running ETL Pipeline
if __name__ == "__main__":
    engine = get_engine()
    df = extract_stock_data(api_key)
    df_stocks_selected = transform_data(df)
    load_data(df_stocks_selected, engine)
