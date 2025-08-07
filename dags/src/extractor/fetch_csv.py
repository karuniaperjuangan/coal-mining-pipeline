from db.clickhouse import ClickHouseConnection
from utils.create_table_clickhouse import create_clickhouse_table_from_df
import pandas as pd
import os

def fetch_csv():

    config_clickhouse ={
        "host": os.environ.get("CLICKHOUSE_HOST"),
        "port":os.environ.get("CLICKHOUSE_PORT"),
        "user": os.environ.get("CLICKHOUSE_USER"),
        "password": os.environ.get("CLICKHOUSE_PASSWORD"),
        "dbname": os.environ.get("CLICKHOUSE_DATABASE"),
    }

    csv_names =["equipment_sensors"]
    for table in csv_names:
            print(os.listdir(".."))
            df_result = pd.read_csv(f"seed/iot/{table}.csv")
            
            ch_conn = ClickHouseConnection(
                        host=config_clickhouse["host"],
                        port=config_clickhouse["port"],
                        user=config_clickhouse["user"],
                        password=config_clickhouse["password"],
                        dbname=config_clickhouse["dbname"]
            )

            ch_conn.connect()

            create_clickhouse_table_from_df(ch_client=ch_conn.get_conn(),
                                            df=df_result,
                                            database_name=config_clickhouse["dbname"],
                                            table_name=table,
                                            cluster_name="'{cluster}'",
                                            primary_keys=["timestamp","equipment_id"]) # manually set for now
            ch_conn.insert_dataframe(f"""INSERT INTO {config_clickhouse["dbname"]}.{table} {str(tuple(df_result.columns)).replace("'","")} VALUES""", df_result)    

if __name__ == "__main__":
      fetch_csv()