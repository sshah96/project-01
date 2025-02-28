import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import DateTime
from datetime import datetime

def transform_timestamp(timestamp_str):
    # Parse the timestamp string into a datetime object
    timestamp_obj = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S%z")
    
    # Format the datetime object into a human-readable string
    readable_format = timestamp_obj.strftime("%B %d, %Y %I:%M %p")
    
    return readable_format

from secrets_config import api_key, db_user, db_password, db_server_name, db_database_name

# Define the API endpoint
api_url = "https://api.marketstack.com/v1/eod"

# Define request parameters dynamically
params = {
    "access_key": api_key,  # MarketStack API Key from secrets_config
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
    
    # Print the DataFrame (or you can return it, save it to CSV, etc.)
    print(df)
else:
    print("Error:", response.status_code, response.text)  # Print error message if request fails


df


df_stocks = df
df_stocks.columns


df_stocks_selected = df_stocks[["open", "close", "volume", "dividend", "symbol", "exchange", "date"]]
df_stocks_selected


df_stocks_selected.dtypes


# Convert Date column to datetime format
df_stocks_selected['date'] = pd.to_datetime(df_stocks_selected['date'])

# Create Unique_ID by combining Symbol and Date
df_stocks_selected['unique_id'] = df_stocks_selected['symbol'] + "_" + df_stocks_selected['date'].dt.strftime('%Y-%m-%d')

# Set Unique_ID as index (optional)
df_stocks_selected.set_index('unique_id', inplace=True)

# Display the first few rows to verify
print(df_stocks_selected.head())


from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float # https://www.tutorialspoint.com/sqlalchemy/sqlalchemy_core_creating_table.htm
from sqlalchemy.engine import URL
from sqlalchemy.dialects import postgresql

from sqlalchemy.schema import CreateTable 


# SQLAlchemy Core Imports
from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy import Integer, String, Float
from sqlalchemy.engine import URL
from sqlalchemy.schema import CreateTable

# Importing PostgreSQL dialect (optional, only if using PostgreSQL-specific types)
# from sqlalchemy.dialects import postgresql

# Importing Secrets (Assuming secrets are stored in secrets_config.py)



# create connection to database 
connection_url = URL.create(
    drivername="postgresql+pg8000", 
    username=db_user,
    password=db_password,
    host=db_server_name, 
    port=5432,
    database=db_database_name
)

# Create the engine
engine = create_engine(connection_url)

# Test the connection
try:
    with engine.connect() as connection:
        print("Connection to the database was successful!")
except Exception as e:
    print(f"Error connecting to the database: {e}")


df_stocks_selected.to_sql("stocks_table", engine, if_exists="replace", index=False)


df_stocks_selected.dtypes


from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float, DateTime

# Define the metadata
meta = MetaData()

# Define the table schema
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



from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import exc

def upsert():
    # Convert DataFrame to list of dictionaries
    data_to_insert = df_stocks_selected.to_dict(orient='records')

    # Check if data_to_insert is not empty
    if not data_to_insert:
        print("No data to insert.")
        return

    # Perform the upsert in batch
    with engine.begin() as conn:  # Use begin() for automatic commit/rollback
        try:
            for row in data_to_insert:
                # Create the insert statement
                insert_statement = insert(stocks_table).values(**row)
                
                # Define the upsert statement
                upsert_statement = insert_statement.on_conflict_do_update(
                    index_elements=['unique_id'],  # Use Unique_ID as the conflict index
                    set_={col.name: col for col in insert_statement.excluded if col.name != 'unique_id'}
                )
                
                # Execute the upsert
                conn.execute(upsert_statement)
            
            print("Upsert completed successfully.")
        
        except exc.SQLAlchemyError as e:
            print(f"Error during upsert: {e}")
