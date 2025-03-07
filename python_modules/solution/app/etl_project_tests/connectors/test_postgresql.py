from python_modules.connectors.postgresql import PostgreSqlClient
import pytest
from dotenv import load_dotenv
import os
from sqlalchemy import Table, Column, Integer, String, MetaData


@pytest.fixture
def setup_postgresql_client():
    load_dotenv()
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    PORT = os.environ.get("PORT")

    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )
    return postgresql_client


@pytest.fixture
def setup_table():
    table_name = "test_table"
    metadata = MetaData()
    table = Table(
        table_name,
        metadata,
        Column("date", Integer, primary_key=True),
        Column("symbol", String),
    )
    return table_name, table, metadata


def test_postgresqlclient_insert(setup_postgresql_client, setup_table):
    postgresql_client = setup_postgresql_client
    table_name, table, metadata = setup_table
    postgresql_client.drop_table(table_name)  

    data = [{"date": "2025-02-24T00:00:00+0000", "symbol": "MSFT"}, {"date": "2025-02-24T00:00:00+0000", "symbol": "NFLX"}]

    postgresql_client.insert(data=data, table=table, metadata=metadata)

    result = postgresql_client.select_all(table=table)
    assert len(result) == 2

    postgresql_client.drop_table(table_name)
