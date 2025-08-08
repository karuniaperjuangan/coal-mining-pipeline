import pendulum
import sys
import os

from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

#import src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

from extractor.fetch_csv import fetch_csv
from extractor.fetch_mysql import fetch_mysql
from extractor.fetch_weather_api import fetch_weather_api

@dag(
    dag_id="coal_mining_etl",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["coal_mining", "synapsis"],
)
def coal_mining_dag():

    start_task = EmptyOperator(task_id="start")

    fetch_csv_task = PythonOperator(
        task_id="fetch_csv",
        python_callable=fetch_csv,
    )

    fetch_mysql_task = PythonOperator(
        task_id="fetch_mysql",
        python_callable=fetch_mysql,
    )

    fetch_weather_task = PythonOperator(
        task_id="fetch_weather_api",
        python_callable=fetch_weather_api,
        op_kwargs={'start_date': os.environ.get("START_DATE", '{{ds}}'), 'end_date': os.environ.get("END_DATE", '{{ds}}')}
    )

    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run",
        cwd=os.path.join(os.path.dirname(__file__), "dbt"),
    )

    dbt_test_task = BashOperator(
        task_id="dbt_test",
        # check failed unit test cases.
        bash_command=f"set -o pipefail; dbt test 2>&1 | tee {os.path.dirname(__file__)}/../logs/dbt_test_failures.log",
        cwd=os.path.join(os.path.dirname(__file__), "dbt"),
    )

    end_task = EmptyOperator(task_id="end")

    start_task >> [fetch_csv_task, fetch_mysql_task, fetch_weather_task] >> dbt_run_task >> dbt_test_task >> end_task

coal_mining_dag_instance = coal_mining_dag()