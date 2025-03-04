# --------------------
# Import Statements
# --------------------
import os
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, Table, Column, String, MetaData, Float, DateTime
from sqlalchemy.engine import URL
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import exc

# --------------------
# Load Environment Variables (Fixed: Loaded Once)
# --------------------
load_dotenv()

api_key = os.getenv('API_KEY')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_server_name = os.getenv('DB_SERVER_NAME')
db_database_name = os.getenv('DB_DATABASE_NAME')

# Validate environment variables
if not all([api_key, db_user, db_password, db_server_name, db_database_name]):
    raise ValueError(" One or more required environment variables are missing.")

# --------------------
# Database Connection (Uses a Function)
# --------------------
def get_engine():
    """
    Creates and returns a SQLAlchemy engine using pg8000.
    """
    connection_url = URL.create(
        drivername="postgresql+pg8000",
        username=db_user,
        password=db_password,
        host=db_server_name,
        port=5432,
        database=db_database_name
    )
    return create_engine(connection_url)

# --------------------
# Extract Data
# --------------------
def extract_stock_data(api_key: str) -> pd.DataFrame:
    """
    Extracts stock data from MarketStack API and returns a DataFrame.
    """
    api_url = "https://api.marketstack.com/v1/eod"

    params = {
        "access_key": api_key,
        "symbols": "AAPL,AMZN,GOOGL,MSFT,NFLX",
        "sort": "DESC",
        "date_from": "2021-01-01",
        "date_to": datetime.today().strftime("%Y-%m-%d"),
        "limit": 5000,
        "offset": 0
    }

    response = requests.get(api_url, params=params)

    if response.status_code == 200:
        data = response.json()
        df = pd.json_normalize(data['data'])
        print("✅ Data extraction completed successfully.")
        return df
    else:
        print(f" Error: {response.status_code} - {response.text}")
        return pd.DataFrame()

# --------------------
# Transform Data (Ensures Date is in UTC)
# --------------------
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms extracted stock data by selecting columns, filtering, and formatting.
    Converts the date column to UTC.
    """
    if df.empty:
        print("⚠️ No data to transform.")
        return pd.DataFrame()

    df_stocks_selected = df.loc[:, ["open", "close", "volume", "dividend", "symbol", "exchange", "date"]].copy()

    df_stocks_selected = df_stocks_selected[
        df_stocks_selected['symbol'].notna() &
        df_stocks_selected['symbol'].ne('') &
        df_stocks_selected['date'].notna()
    ]
     
    # Convert Date column to datetime format and set to UTC
    df_stocks_selected['date'] = pd.to_datetime(df_stocks_selected['date'], errors='coerce').dt.tz_localize(None).dt.tz_localize('UTC')

    df_stocks_selected = df_stocks_selected.dropna(subset=['date'])
    df_stocks_selected['unique_id'] = df_stocks_selected['symbol'] + "_" + df_stocks_selected['date'].dt.strftime('%Y-%m-%d')

    df_stocks_selected.reset_index(drop=True, inplace=True)
    print("✅ Data transformation completed successfully. (Time Zone: UTC)")
    return df_stocks_selected

# --------------------
# Create Table Schema (Uses a Function)
# --------------------
def create_table(engine):
    """
    Creates the stocks_data table if it does not exist.
    """
    meta = MetaData()

    stocks_table = Table(
        "stocks_data", meta,
        Column("unique_id", String, primary_key=True),
        Column("open", Float),
        Column("close", Float),
        Column("volume", Float),
        Column("dividend", Float),
        Column("symbol", String),
        Column("exchange", String),
        Column("date", DateTime(timezone=True))  # Ensures date is stored in UTC
    )

    meta.create_all(engine)
    return stocks_table

# --------------------
# Load Data Using Batches to Avoid struct.error
# --------------------
def load_data(df_stocks_selected, engine, batch_size=1000):
    """
    Loads transformed data into PostgreSQL using batched insert and upsert.
    Fixes struct.error: 'h' format requires -32768 <= number <= 32767.
    """
    if df_stocks_selected.empty:
        print("⚠️ No data to insert.")
        return

    stocks_table = create_table(engine)

    # Convert DataFrame to list of dictionaries
    data_to_insert = df_stocks_selected.to_dict(orient='records')

    # Perform the bulk upsert in batches to avoid exceeding parameter limits
    with engine.connect() as conn:
        try:
            for i in range(0, len(data_to_insert), batch_size):
                batch = data_to_insert[i:i + batch_size]

                # Create the insert statement
                insert_statement = insert(stocks_table).values(batch)

                # Define upsert (update if key exists)
                upsert_statement = insert_statement.on_conflict_do_update(
                    index_elements=['unique_id'],
                    set_={col.name: col for col in insert_statement.excluded if col.name != 'unique_id'}
                )

                conn.execute(upsert_statement)

            print("✅ Bulk upsert completed successfully.")
        except exc.SQLAlchemyError as e:
            print(f" Error during upsert: {e}")

# --------------------
# Main Execution Block
# --------------------
if __name__ == "__main__":
    # Create Engine
    engine = get_engine()

    # Create Table if not exists
    create_table(engine)

    # Extract Data
    df = extract_stock_data(api_key)

    # Transform Data (Now ensures time zone is UTC)
    df_stocks_selected = transform_data(df)

    # Load Data into the Database
    load_data(df_stocks_selected, engine)
