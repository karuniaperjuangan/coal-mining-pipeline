from db.mysql import MySQLConnection
from db.clickhouse import ClickHouseConnection
from utils.create_table_clickhouse import create_clickhouse_table_from_mysql
import pandas as pd
import os

def fetch_mysql():
    config_mysql ={
        "host": os.environ.get("MYSQL_HOST"),
        "port":os.environ.get("MYSQL_PORT"),
        "user": os.environ.get("MYSQL_USER"),
        "password": os.environ.get("MYSQL_PASSWORD"),
        "dbname": os.environ.get("MYSQL_DATABASE"),
    }

    config_clickhouse ={
        "host": os.environ.get("CLICKHOUSE_HOST"),
        "port":os.environ.get("CLICKHOUSE_PORT"),
        "user": os.environ.get("CLICKHOUSE_USER"),
        "password": os.environ.get("CLICKHOUSE_PASSWORD"),
        "dbname": os.environ.get("CLICKHOUSE_DATABASE"),
    }

    mysql_conn = MySQLConnection(
        host=config_mysql["host"],
        port=config_mysql["port"],
        user=config_mysql["user"],
        password=config_mysql["password"],
        dbname=config_mysql["dbname"]
    )

    mysql_conn.connect()

    table_names = ["mines","production_logs"]
    for table in table_names:
            df_result = pd.read_sql(f"SELECT * FROM {table}", mysql_conn.get_conn())
            
            ch_conn = ClickHouseConnection(
                        host=config_clickhouse["host"],
                        port=config_clickhouse["port"],
                        user=config_clickhouse["user"],
                        password=config_clickhouse["password"],
                        dbname=config_clickhouse["dbname"]
            )

            ch_conn.connect()

            create_clickhouse_table_from_mysql(ch_client=ch_conn.get_conn(),
                                    mysql_cursor=mysql_conn.get_conn().cursor(dictionary=True),
                                    mysql_database=config_mysql["dbname"],
                                    clickhouse_database=config_clickhouse["dbname"],
                                    table_name=table,
                                    cluster_name="'{cluster}'"
                                    )
            ch_conn.insert_dataframe(f"""INSERT INTO {config_clickhouse["dbname"]}.{table} {str(tuple(df_result.columns)).replace("'","")} VALUES""", df_result)    

if __name__ == "__main__":
      fetch_mysql()