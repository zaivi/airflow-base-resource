import pickle
from datetime import datetime, timedelta
from io import StringIO

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.cloud import bigquery

PROJECT_ID = Variable.get(key="project_id", default_var=None)


def get_dag_runs(**kwargs):
    # Hooks
    db_hook = PostgresHook("airflow_db")
    bq_hook = BigQueryHook()

    # Define data interval
    start_date = pendulum.parse(kwargs["ds"])
    end_date = start_date.add(days=1)
    print(f"Extracting dag runs from {start_date} to {end_date}")

    # Get dag run data from airflow_db
    sql_1 = """SELECT id, dag_id, queued_at, execution_date, start_date, end_date, state, run_id, external_trigger, run_type, data_interval_start, data_interval_end, updated_at, conf
            FROM dag_run WHERE end_date >= %(start_date)s AND end_date < %(end_date)s
    """
    df_dag_runs = db_hook.get_pandas_df(sql_1, parameters={"start_date": start_date, "end_date": end_date})

    if len(df_dag_runs) == 0:
        return "No dag runs found"
    else:
        # conf is only available for triggered dag runs
        df_dag_runs["conf"] = df_dag_runs.loc[df_dag_runs["conf"].notnull(), "conf"].apply(lambda x: pickle.loads(x))
        print(df_dag_runs.head())

        # Get dag tags data from airflow db
        sql_2 = """SELECT * FROM dag_tag"""
        df_dag_tags = db_hook.get_pandas_df(sql_2)
        df_dag_tags = df_dag_tags.groupby("dag_id", as_index=False)["name"].apply(lambda x: ",".join(x))
        df_dag_tags.rename(columns={"name": "tags"}, inplace=True)
        print(df_dag_tags.head())

        # Merge dataframes
        df_dag_runs = df_dag_runs.merge(df_dag_tags, on="dag_id", how="left")
        df_dag_runs["project_id"] = PROJECT_ID
        print(df_dag_runs.head())

        # df_dag_runs.to_json('/home/airflow/gcs/data/dag_runs.jsonl', orient='records', lines=True, date_format='iso')

        # Insert into BigQuery
        # cannot use load_table_from_dataframe because of JSON column in `conf`
        buffer = StringIO()
        df_dag_runs.to_json(buffer, orient="records", lines=True, date_format="iso")
        buffer.seek(0)
        client = bq_hook.get_client()
        table_id = f"<YOUR_TABLE_ID>${kwargs['ds_nodash']}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, write_disposition="WRITE_TRUNCATE"
        )
        job = client.load_table_from_file(buffer, table_id, job_config=job_config)
        job.result()


default_args = {
    "start_date": datetime(2023, 11, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="export_dag_runs",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    max_active_runs=1,
    catchup=True,
) as dag:
    t1 = PythonOperator(task_id="get_dag_runs", python_callable=get_dag_runs)
