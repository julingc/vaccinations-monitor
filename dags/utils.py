import os

import pandas as pd
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

# Config variables
BQ_CONN_ID = "bigquery_connection"
BQ_PROJECT = "vaccination-monitor"
BQ_DATASET = "vaccinations"
table_id = "vaccination-monitor.vaccinations.daily-vaccinations"


def get_vaccination_data(url: str, filename: str):
    df = pd.read_csv(url)
    df = df[["location", "date", "daily_vaccinations_per_million"]]
    df["date"] = pd.to_datetime(df["date"])

    # Extract necessary data and output a csv for loading to BigQuery
    df.to_csv(filename, index=False)
    csv_rows = len(df.index)
    return csv_rows


def check_bq_rows():
    hook = BigQueryHook(bigquery_conn_id=BQ_CONN_ID, use_legacy_sql=False)
    client = bigquery.Client(
        project=hook._get_field("project"), credentials=hook._get_credentials()
    )
    table = client.get_table(table_id)
    bq_rows = table.num_rows
    return bq_rows


def compare(**context):
    csv_rows = context["task_instance"].xcom_pull(task_ids="extract_vaccination_data")
    bq_rows = context["task_instance"].xcom_pull(task_ids="get_bq_rows")

    if csv_rows == bq_rows:
        return "remove_csv"
    else:
        return "vaccination_data_to_bq"


def load_data_to_bq(filename: str, remove_local=False):
    vaccinations_df = pd.read_csv(filename)
    vaccinations_df["date"] = pd.to_datetime(vaccinations_df["date"])

    hook = BigQueryHook(bigquery_conn_id=BQ_CONN_ID, use_legacy_sql=False)
    client = bigquery.Client(
        project=hook._get_field("project"), credentials=hook._get_credentials()
    )

    # Overwrite table data with the lastest vaccinations.csv
    job_config = bigquery.job.LoadJobConfig(
        schema=[
            bigquery.SchemaField("location", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.DATE),
            bigquery.SchemaField(
                "daily_vaccinations_per_million", bigquery.enums.SqlTypeNames.INTEGER
            ),
        ],
        write_disposition="WRITE_TRUNCATE",
    )
    load_job = client.load_table_from_dataframe(
        vaccinations_df, table_id, job_config=job_config
    )
    load_job.result()

    # Remove local csv file once data loaded to bigquery
    if remove_local:
        if os.path.isfile(filename):
            os.remove(filename)


def remove_local(filename: str):
    if os.path.isfile(filename):
        os.remove(filename)
