from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

from utils import (
    check_bq_rows,
    compare,
    get_vaccination_data,
    load_data_to_bq,
    remove_local,
)

# DAG definition
default_args = {
    "owner": "Juling",
    "email": ["julingjchang@gmail.com"],
    "start_date": datetime(2021, 8, 28),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG("etl_workflow", default_args=default_args, schedule_interval="@daily")

# Config variables
BQ_CONN_ID = "bigquery_connection"
BQ_PROJECT = "vaccination-monitor"
BQ_DATASET = "vaccinations"

extract_vaccination_data = PythonOperator(
    dag=dag,
    task_id="extract_vaccination_data",
    python_callable=get_vaccination_data,
    op_kwargs={
        "url": "https://github.com/owid/covid-19-data/raw/master/public/data/vaccinations/vaccinations.csv?raw=true",
        "filename": "temp/vaccinations.csv",
    },
)

get_bq_rows = PythonOperator(
    dag=dag,
    task_id="get_bq_rows",
    python_callable=check_bq_rows,
)

compare_num_rows = BranchPythonOperator(
    dag=dag,
    task_id="compare_num_rows",
    python_callable=compare,
)

vaccination_data_to_bq = PythonOperator(
    dag=dag,
    task_id="vaccination_data_to_bq",
    python_callable=load_data_to_bq,
    op_kwargs={
        "filename": "temp/vaccinations.csv",
        "remove_local": "true",
    },
)

remove_csv = PythonOperator(
    dag=dag,
    task_id="remove_csv",
    python_callable=remove_local,
    op_kwargs={"filename": "temp/vaccinations.csv"},
)
end_of_data_pipeline = DummyOperator(task_id="end_of_data_pipeline", dag=dag)

extract_vaccination_data >> get_bq_rows >> compare_num_rows
compare_num_rows >> vaccination_data_to_bq >> end_of_data_pipeline
compare_num_rows >> remove_csv
