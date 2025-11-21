import requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

def fetch_crypto_prices():
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin,ethereum,dogecoin",
        "vs_currencies": "usd"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    rows = []
    for coin, values in data.items():
        rows.append({
            "coin": coin,
            "price_usd": values["usd"],
            "timestamp": datetime.utcnow()
        })

    return pd.DataFrame(rows)


def upload_to_bigquery(df):
    client = bigquery.Client()

    table_id = "<YOUR_PROJECT>.<YOUR_DATASET>.crypto_prices"

    job = client.load_table_from_dataframe(df, table_id)
    job.result()

    print("Data uploaded to BigQuery:", table_id)


if __name__ == "__main__":
    df = fetch_crypto_prices()
    print(df)
    upload_to_bigquery(df)
