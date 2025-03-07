import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import DateTime
from datetime import datetime

def transform_timestamp(timestamp_str):
    # Parse the timestamp string into a datetime object
    timestamp_obj = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S%z")
    
    # Formatting the datetime object into a human-readable string
    readable_format = timestamp_obj.strftime("%B %d, %Y %I:%M %p")
    
    return readable_format

from secrets_config import api_key, db_user, db_password, db_server_name, db_database_name

# Defining the API endpoint
api_url = "https://api.marketstack.com/v1/eod"

# Defining request parameters dynamically
params = {
    "access_key": api_key,  
    "symbols": "AAPL,AMZN,GOOGL,MSFT,NFLX", 
    "sort": "DESC",  
    "date_from": "2021-01-01",  
    "date_to": datetime.today().strftime("%Y-%m-%d"), 
    "limit": 1000,  
    "offset": 0  
}

# Sending GET request to the MarketStack API
response = requests.get(api_url, params=params)

# Checking if the response is successful (status code 200)
if response.status_code == 200:
    data = response.json()  
    # Normalize the 'data' field to create a DataFrame
    df = pd.json_normalize(data['data'])
    
    # Printing the DataFrame 
    print(df)
else:
    print("Error:", response.status_code, response.text)  


df


df_stocks = df
df_stocks.columns


df_stocks_selected = df_stocks[["open", "close", "volume", "dividend", "symbol", "exchange", "date"]]
df_stocks_selected


df_stocks_selected.dtypes


# Converting Date column to datetime format
df_stocks_selected['date'] = pd.to_datetime(df_stocks_selected['date'])

# Creating Unique_ID by combining Symbol and Date
df_stocks_selected['unique_id'] = df_stocks_selected['symbol'] + "_" + df_stocks_selected['date'].dt.strftime('%Y-%m-%d')

# Setting Unique_ID as index 
df_stocks_selected.set_index('unique_id', inplace=True)

# Displaying the first few rows to verify
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


# creating connection to database 
connection_url = URL.create(
    drivername="postgresql+pg8000", 
    username=db_user,
    password=db_password,
    host=db_server_name, 
    port=5432,
    database=db_database_name
)

# Creating the engine
engine = create_engine(connection_url)

# Testing the connection
try:
    with engine.connect() as connection:
        print("Connection to the database was successful!")
except Exception as e:
    print(f"Error connecting to the database: {e}")


df_stocks_selected.to_sql("stocks_table", engine, if_exists="replace", index=False)


df_stocks_selected.dtypes


from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float, DateTime

# Defining the metadata
meta = MetaData()

# Defining the table schema
stocks_table = Table(
    "stocks_table", meta, 
    Column("unique_id", String, primary_key=True),  
    Column("open", Float),
    Column("close", Float),
    Column("volume", Float),
    Column("dividend", Float),
    Column("symbol", String),       
    Column("exchange", String),     
    Column("date", DateTime(timezone=True))  
    )

# Creating the table if it does not exist
meta.create_all(engine)



from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import exc

def upsert():
    # Converting DataFrame to list of dictionaries
    data_to_insert = df_stocks_selected.to_dict(orient='records')

    # Checking if data_to_insert is not empty
    if not data_to_insert:
        print("No data to insert.")
        return

    # Performing the upsert in batch
    with engine.begin() as conn:  
        try:
            for row in data_to_insert:
                # Creating the insert statement
                insert_statement = insert(stocks_table).values(**row)
                
                # Defining the upsert statement
                upsert_statement = insert_statement.on_conflict_do_update(
                    index_elements=['unique_id'], 
                    set_={col.name: col for col in insert_statement.excluded if col.name != 'unique_id'}
                )
                
                # Executing the upsert
                conn.execute(upsert_statement)
            
            print("Upsert completed successfully.")
        
        except exc.SQLAlchemyError as e:
            print(f"Error during upsert: {e}")
