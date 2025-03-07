import os
import pandas as pd
from sqlalchemy import create_engine, Table, Column, String, MetaData, Float, DateTime
from sqlalchemy.engine import URL
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import exc
from dotenv import load_dotenv

# Loading environment variables
load_dotenv()

db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_server_name = os.getenv('DB_SERVER_NAME')
db_database_name = os.getenv('DB_DATABASE_NAME')
db_port = int(os.getenv('PORT', 5432))  

def get_engine():
    """
    Creates and returns a SQLAlchemy engine using pg8000.
    """
    connection_url = URL.create(
        drivername="postgresql+pg8000",
        username=db_user,
        password=db_password,
        host=db_server_name,
        port=db_port,
        database=db_database_name
    )
    return create_engine(connection_url, pool_pre_ping=True, echo=False)

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
        Column("date", DateTime(timezone=True))  # This ensures date is stored in UTC
    )

    with engine.connect() as conn:
        meta.create_all(conn)
    return stocks_table

def load_data(df_stocks_selected, engine, batch_size=1000):
    """
    Loads transformed data into PostgreSQL using batched insert and upsert.
    """
    if df_stocks_selected.empty:
        print("No data to insert.")
        return

    stocks_table = create_table(engine)

    # Converting DataFrame to list of dictionaries
    data_to_insert = df_stocks_selected.to_dict(orient='records')

    with engine.connect() as conn:
        try:
            for i in range(0, len(data_to_insert), batch_size):
                batch = data_to_insert[i:i + batch_size]

                # Creating the insert statement
                insert_statement = insert(stocks_table).values(batch)

                # Defining upsert (update if key exists)
                upsert_statement = insert_statement.on_conflict_do_update(
                    index_elements=['unique_id'],
                    set_={col.name: col for col in insert_statement.excluded if col.name != 'unique_id'}
                )

                conn.execute(upsert_statement)

            print("Bulk upsert completed successfully.")
        except exc.SQLAlchemyError as e:
            print(f" Error during upsert: {e}")
