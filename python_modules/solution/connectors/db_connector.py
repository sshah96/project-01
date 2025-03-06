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
def get_engine(db_user, db_password, db_server_name, db_database_name):
    """
    Creates and returns a SQLAlchemy engine using pg8000 as the driver.
    """
    connection_url = URL.create(
        drivername="postgresql+pg8000",  # Using pg8000 instead of psycopg2
        username=db_user,
        password=db_password,
        host=db_server_name, 
        port=5432,
        database=db_database_name
    )
    engine = create_engine(connection_url)
    return engine

# --------------------
# Step 3: Load Data with Bulk Upsert
# --------------------
def load_data(df_stocks_selected, engine, batch_size=10000):
    """
    Loads the transformed data into PostgreSQL using bulk upsert logic.
    Uses batching for large datasets.
    """
    # Define the metadata and table schema inside the function
    meta = MetaData()

    stocks_table = Table(
        "my_stocks_table", meta, 
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
