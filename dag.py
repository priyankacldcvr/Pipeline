import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

# Custom Python logic for derriving data value
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

# Default arguments
default_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# DAG definitions
with DAG(dag_id='GCS_to_BQ',
         catchup=False,
         schedule_interval=timedelta(minutes=1),
         default_args=default_args
         ) as dag:

# Dummy strat task   
    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

# GCS to BigQuery data load Operator and task
    gcs_to_bq_load = GoogleCloudStorageToBigQueryOperator(
                task_id='gcs_to_bq_load',
                bucket='batchdatatask-1',
                source_objects=['Walmart.csv'],
                destination_project_dataset_table='priyankachoudhary-1680237339.taskpc1dataset.taskpc1table',
                schema_fields=[],
                skip_leading_rows=1,
                create_disposition='CREATE_IF_NEEDED',
                write_disposition='WRITE_TRUNCATE',
                autodetect=True,
                dag=dag)

# BigQuery task, operator
    create_aggr_bq_table = BigQueryOperator(
    task_id='create_aggr_bq_table',
    use_legacy_sql=False,
    allow_large_results=True,
    sql="CREATE OR REPLACE TABLE taskpc1dataset.copytab AS \
         SELECT * FROM taskpc1dataset.taskpc1table", 
    dag=dag)

# Dummy end task
    end = DummyOperator(
        task_id='end',
        dag=dag,
    )

# Settting up task  dependency
start >> gcs_to_bq_load >> create_aggr_bq_table >> end
