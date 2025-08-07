import requests
import pandas as pd
from db.clickhouse import ClickHouseConnection
from utils.create_table_clickhouse import create_clickhouse_table_from_df
import os

def fetch_weather_api(start_date,end_date):
    
    config_clickhouse ={
        "host": os.environ.get("CLICKHOUSE_HOST"),
        "port":os.environ.get("CLICKHOUSE_PORT"),
        "user": os.environ.get("CLICKHOUSE_USER"),
        "password": os.environ.get("CLICKHOUSE_PASSWORD"),
        "dbname": os.environ.get("CLICKHOUSE_DATABASE"),
    }
        
    response = requests.get(f"https://archive-api.open-meteo.com/v1/archive?latitude=2.0167&longitude=117.3&start_date={start_date}&end_date={end_date}&daily=temperature_2m_mean,precipitation_sum&timezone=Asia%2FJakarta")
    data = response.json()["daily"]

    df_result = pd.DataFrame.from_dict(data=data,orient="columns")

    ch_conn = ClickHouseConnection(
                        host=config_clickhouse["host"],
                        port=config_clickhouse["port"],
                        user=config_clickhouse["user"],
                        password=config_clickhouse["password"],
                        dbname=config_clickhouse["dbname"]
            )

    ch_conn.connect()

    table = "weather"
    create_clickhouse_table_from_df(ch_client=ch_conn.get_conn(),
                                    df=df_result,
                                    database_name=config_clickhouse["dbname"],
                                    table_name=table,
                                    cluster_name="'{cluster}'",
                                    primary_keys=["time"]) # manually set for now
    ch_conn.insert_dataframe(f"""INSERT INTO {config_clickhouse["dbname"]}.{table} {str(tuple(df_result.columns)).replace("'","")} VALUES""", df_result)    

