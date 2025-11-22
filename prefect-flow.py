from prefect import flow, task
import requests, duckdb, pandas as pd
from datetime import date

API_URL = "https://urlhaus.abuse.ch/api/"
AUTH_KEY = "YOUR-AUTH-KEY"

@task
def fetch_urlhaus_data(url_list):
    payload = {"urls": url_list, "auth_key": AUTH_KEY}
    r = requests.post(f"{API_URL}v1/url_info/", json=payload)
    r.raise_for_status()
    return r.json()

@task
def transform_to_df(api_response):
    df = pd.json_normalize(api_response["urls"])
    df["date_processed"] = date.today()
    return df

@task
def store_duckdb(df):
    conn = duckdb.connect("data/urlhaus.duckdb")
    conn.execute("CREATE TABLE IF NOT EXISTS urls AS SELECT * FROM df LIMIT 0")
    conn.execute("INSERT INTO urls SELECT * FROM df")
    conn.close()

@flow
def urlhaus_pipeline():
    url_list = [  # could also read from url-list.txt
        "http://malicious.example1.com",
        "http://malicious.example2.net"
    ]
    data = fetch_urlhaus_data(url_list)
    df = transform_to_df(data)
    store_duckdb(df)

if __name__ == "__main__":
    urlhaus_pipeline()
