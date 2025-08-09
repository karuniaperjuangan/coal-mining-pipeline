import os
import numpy as np
import pandas as pd
from db.clickhouse import ClickHouseConnection
from forecaster.forecaster import Forecaster
from utils.create_table_clickhouse import create_clickhouse_table_from_df

def forecast_production():
    config_clickhouse = {
        "host": os.environ.get("CLICKHOUSE_HOST"),
        "port": os.environ.get("CLICKHOUSE_PORT"),
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

    try:
        data, columns = ch_conn.execute_query(
            "SELECT date, total_production_daily, precipitation_sum FROM default_gold.daily_production_metrics",
            with_column_types=True
        )
        columns = [col[0] for col in columns]
        df = pd.DataFrame(data, columns=columns)
        df = df.rename(columns={'date': 'ds', 'total_production_daily': 'y'}) # format to follow prophet

        forecaster = Forecaster(df, regressors=['precipitation_sum']) #using weather to predict total_production daily
        forecaster.create_prophet_model()
        forecast = forecaster.predict_future(periods=365)

        forecasted_df = forecast[['ds', 'yhat']].rename(columns={'ds': 'date', 'yhat': 'total_production_daily'})
        forecasted_df['label'] = 'predicted'
        
        original_columns = [col for col in columns if col not in ['date', 'total_production_daily']]
        for col in original_columns:
            if col == 'precipitation_sum' and 'precipitation_sum' in forecast.columns:
                 forecasted_df[col] = forecast[col]
            else:
                forecasted_df[col] = np.nan # if not predicted, set to nan

        # Prepare the actual data, muse default prophet formatting with ds as timestamp and y as predicted value
        actual_df = df.rename(columns={'ds': 'date', 'y': 'total_production_daily'})
        actual_df['label'] = 'actual'
        
        # concat original and predicted data
        final_df = pd.concat([actual_df, forecasted_df.tail(365)]) # 1 year prediction
        final_df = final_df.rename(columns={'y': 'total_production_daily', 'ds': 'date'})


        table_name = "default_gold.forecasted_production_metrics"
        db_name, tb_name = table_name.split('.')
        
        ch_conn.execute_query(f"DROP TABLE IF EXISTS {table_name}")

        final_df['date'] = pd.to_datetime(final_df['date'])
        #conver datetime columns as YYYY-MM-DD string
        final_df['date'] = final_df['date'].dt.strftime("%Y-%m-%d")

        create_clickhouse_table_from_df(ch_conn.get_conn(), final_df, db_name, tb_name, primary_keys=['date', 'label'])
        ch_conn.insert_dataframe(f"""INSERT INTO {db_name}.{tb_name} {str(tuple(final_df.columns)).replace("'","")} VALUES""", final_df)    

    finally:
        ch_conn.close()
    


