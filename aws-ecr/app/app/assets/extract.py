import os
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Loading environment variables
load_dotenv()
api_key = os.getenv("API_KEY")

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
        print("Data extraction completed successfully.")
        return df
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return pd.DataFrame()

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms extracted stock data by selecting columns, filtering, and formatting.
    Converts the date column to UTC.
    """
    if df.empty:
        print("No data to transform.")
        return pd.DataFrame()

    df_stocks_selected = df.loc[:, ["open", "close", "volume", "dividend", "symbol", "exchange", "date"]].copy()

    df_stocks_selected = df_stocks_selected[
        df_stocks_selected['symbol'].notna() & df_stocks_selected['symbol'].ne('') & df_stocks_selected['date'].notna()
    ]

    # Converting Date column to datetime format and set to UTC
    df_stocks_selected['date'] = pd.to_datetime(df_stocks_selected['date'], errors='coerce').dt.tz_localize(None).dt.tz_localize('UTC')

    df_stocks_selected = df_stocks_selected.dropna(subset=['date'])
    df_stocks_selected['unique_id'] = df_stocks_selected['symbol'] + "_" + df_stocks_selected['date'].dt.strftime('%Y-%m-%d')

    df_stocks_selected.reset_index(drop=True, inplace=True)
    print("Data transformation completed successfully. (Time Zone: UTC)")
    return df_stocks_selected
