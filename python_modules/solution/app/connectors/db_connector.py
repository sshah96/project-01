# --------------------
# Import Statements
# --------------------
from sqlalchemy import create_engine, Table, Column, String, MetaData, Float, DateTime
from sqlalchemy.engine import URL
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import exc

# --------------------
# Database Connection
# --------------------
def get_engine(db_user, db_password, db_server_name, db_database_name, port=5433):
    """
    Creates and returns a SQLAlchemy engine using pg8000 as the driver.
    """
    connection_url = URL.create(
        drivername="postgresql+pg8000",  # Ensure pg8000 is used
        username=db_user,
        password=db_password,
        host=db_server_name, 
        port=port,
        database=db_database_name
    )
    engine = create_engine(connection_url)
    return engine

# --------------------
# Load Data into PostgreSQL
# --------------------
def load_data(df_stocks_selected, engine):
    """
    Loads the transformed data into PostgreSQL using bulk upsert logic.
    """
    # Define metadata and table schema
    meta = MetaData()

    stocks_table = Table(
        "stocks", meta, 
        Column("unique_id", String, primary_key=True),  # Primary key
        Column("open", Float),
        Column("close", Float),
        Column("volume", Float),
        Column("dividend", Float),
        Column("symbol", String),
        Column("exchange", String),
        Column("date", DateTime(timezone=True))
    )

    # Create table if not exists
    meta.create_all(engine)

    # Convert DataFrame to dictionary
    data_to_insert = df_stocks_selected.to_dict(orient='records')

    # Ensure data exists
    if not data_to_insert:
        print("No data to insert.")
        return

    # Perform bulk upsert
    with engine.connect() as conn:
        try:
            insert_statement = insert(stocks_table).values(data_to_insert)

            # Upsert logic
            upsert_statement = insert_statement.on_conflict_do_update(
                index_elements=['unique_id'], 
                set_={col.name: col for col in insert_statement.excluded if col.name != 'unique_id'}
            )

            conn.execute(upsert_statement)
            print("Bulk upsert completed successfully.")

        except exc.SQLAlchemyError as e:
            print(f"Error during bulk upsert: {e}")
