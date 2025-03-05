# --------------------
# Import Statements
# --------------------
import requests
import pandas as pd
from datetime import datetime
import os
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
# --------------------
# Step 1: Extract Data
# --------------------
def extract_stock_data(api_key: str) -> pd.DataFrame:
    """
    Extracts stock data from MarketStack API.
    Returns a DataFrame with the extracted data.
    """
    # Define the API endpoint
    api_url = "https://api.marketstack.com/v1/eod"

    # Define request parameters dynamically
    params = {
        "access_key": api_key,  # MarketStack API Key from .env
        "symbols": "AAPL,AMZN,GOOGL,MSFT,NFLX",  # Stock symbols
        "sort": "DESC",  # Sort results from latest to oldest
        "date_from": "2021-01-01",  # Start date (YYYY-MM-DD)
        "date_to": datetime.today().strftime("%Y-%m-%d"),  # End date (YYYY-MM-DD)
        "limit": 1000,  # Number of results per request (max: 1000 for paid plans)
        "offset": 0  # Pagination offset (0 starts from the first record)
    }

    # Send GET request to the MarketStack API
    response = requests.get(api_url, params=params)

    # Check if the response is successful (status code 200)
    if response.status_code == 200:
        data = response.json()  # Convert response to JSON
        # Normalize the 'data' field to create a DataFrame
        df = pd.json_normalize(data['data'])
        print("Data extraction completed successfully.")
        return df
    else:
        print("Error:", response.status_code, response.text)  # Print error message if request fails
        return pd.DataFrame()  # Return an empty DataFrame on error

# --------------------
# Step 2: Transform Data
# --------------------
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the extracted stock data.
    - Selects relevant columns
    - Drops rows with NULL or empty Symbol or Date
    - Converts Date to datetime format
    - Creates a Unique ID by combining Symbol and Date
    """
    if df.empty:
        print("No data to transform.")
        return pd.DataFrame()

    # Select relevant columns and create a copy to avoid SettingWithCopyWarning
    df_stocks_selected = df.loc[:, ["open", "close", "volume", "dividend", "symbol", "exchange", "date"]].copy()

    # Remove rows with missing or empty Symbol or Date
    df_stocks_selected = df_stocks_selected[
        df_stocks_selected['symbol'].notna() & 
        df_stocks_selected['symbol'].ne('') &
        df_stocks_selected['date'].notna()
    ]

    # Convert Date column to datetime format
    df_stocks_selected['date'] = pd.to_datetime(df_stocks_selected['date'], errors='coerce')

    # Drop rows where Date conversion failed and became NaT (Not a Time)
    df_stocks_selected = df_stocks_selected.dropna(subset=['date'])

    # Create Unique_ID by combining Symbol and Date
    df_stocks_selected['unique_id'] = df_stocks_selected['symbol'] + "_" + df_stocks_selected['date'].dt.strftime('%Y-%m-%d')

    # Reset index to include unique_id as a column
    df_stocks_selected.reset_index(drop=True, inplace=True)

    print("Data transformation completed successfully.")
    return df_stocks_selected



# --------------------
# Step 3: Load Data with Bulk Upsert
# --------------------
def load_data(df_stocks_selected, engine, batch_size=1000):
    """
    Loads the transformed data into PostgreSQL using bulk upsert logic.
    Uses batching for large datasets.
    """
    # Define the metadata and table schema inside the function
    meta = MetaData()

    stocks_table = Table(
        "stocks_table", meta, 
        Column("unique_id", String, primary_key=True),  # Use Unique_ID as the primary key
        Column("open", Float),
        Column("close", Float),
        Column("volume", Float),
        Column("dividend", Float),
        Column("symbol", String),       # Symbol as String
        Column("exchange", String),     # Exchange as String
        Column("date", DateTime(timezone=True))  # Date as DateTime with timezone
    )

    # Create the table if it does not exist
    meta.create_all(engine)

    # Convert DataFrame to list of dictionaries
    data_to_insert = df_stocks_selected.to_dict(orient='records')

    # Check if data_to_insert is not empty
    if not data_to_insert:
        print("No data to insert.")
        return

    # Perform the bulk upsert in batches
    with engine.connect() as conn:
        try:
            for i in range(0, len(data_to_insert), batch_size):
                batch = data_to_insert[i:i+batch_size]

                # Create the insert statement
                insert_statement = insert(stocks_table).values(batch)
                
                # Define the upsert statement
                upsert_statement = insert_statement.on_conflict_do_update(
                    index_elements=['unique_id'],  # Use Unique_ID as the conflict index
                    set_={col.name: col for col in insert_statement.excluded if col.name != 'unique_id'}
                )
                
                # Execute the upsert in batch
                conn.execute(upsert_statement)
            
            print("Bulk upsert completed successfully.")
        
        except exc.SQLAlchemyError as e:
            print(f"Error during bulk upsert: {e}")
