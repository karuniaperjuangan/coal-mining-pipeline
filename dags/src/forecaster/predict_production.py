import os

import pandas as pd
from db.clickhouse import ClickHouseConnection
import forecaster.forecaster as fc

def forecast_production():
    fr = fc.Forecaster()
    
    config_clickhouse ={
        "host": os.environ.get("CLICKHOUSE_HOST"),
        "port":os.environ.get("CLICKHOUSE_PORT"),
        "user": os.environ.get("CLICKHOUSE_USER"),
        "password": os.environ.get("CLICKHOUSE_PASSWORD"),
        "dbname": os.environ.get("CLICKHOUSE_DATABASE"),
    }    

    ch_conn = ClickHouseConnection(
                host=config_clickhouse["host"],
                port=config_clickhouse["port"],
                user=config_clickhouse["user"],
                password=config_clickhouse["password"],
                dbname=config_clickhouse["dbname"]
    )
    

    ch_conn.connect()

    data, columns = ch_conn.execute_query(
        """
        SELECT * from default_gold.daily_production_metrics
        """
    )
    df = pd.DataFrame(data, columns=columns)
    


