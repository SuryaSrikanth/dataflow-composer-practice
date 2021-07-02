import datetime

from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator, BigQueryExecuteQueryOperator)
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import \
    BigQueryToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import \
    GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import \
    GCSToGCSOperator
from airflow.utils.dates import days_ago

PROJECT_ID = 'sudarshan-training'
DATASET_NAME = 'composer_training'
TABLE_NAME = 'temp_table'
TABLE = 'temp1'
BUCKET_NAME = 'gcp-training-composer'
FILE_OBJ = 'assignment.csv'
BUCKET_NAME_ARCHIVE = 'gcp-training-composer-archive'

def print_string(to_print):
    print(to_print)
    return True


dag = models.DAG(
    dag_id='assignment1_surya',
    start_date=days_ago(1),
    schedule_interval=datetime.timedelta(days=1),
    tags=['example'],
)


start = PythonOperator(
        task_id='start',
        python_callable=print_string,
        op_kwargs={'to_print': 'START'},
    )

load_csv = GCSToBigQueryOperator(
    task_id='gcs_to_bigquery',
    bucket=f"{BUCKET_NAME}",
    source_objects=['assignment.csv'],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
    schema_fields=[
        {"name": "invoice_and_item_number", "type": "STRING"},
        {"name": "date", "type": "STRING"},
        {"name": "store_number", "type": "STRING"},
        {"name": "store_name", "type": "STRING"},
        {"name": "address", "type": "STRING"},
        {"name": "city", "type": "STRING"},
        {"name": "zip_code", "type": "STRING"},
        {"name": "store_location", "type": "STRING"},
        {"name": "county_number", "type": "STRING"},
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

save_sql_query = BigQueryExecuteQueryOperator(
    task_id="save_sql_query",
    sql=f"SELECT * FROM {DATASET_NAME}.{TABLE_NAME} where zip_code='50314'",
    use_legacy_sql=False,
    destination_dataset_table=f"{DATASET_NAME}.{TABLE}",
    write_disposition='WRITE_TRUNCATE',
    dag=dag,

)

bigquery_to_gcs = BigQueryToGCSOperator(
    task_id="bigquery_to_gcs",
    source_project_dataset_table=f"{DATASET_NAME}.{TABLE}",
    destination_cloud_storage_uris=[
        "gs://gcp-training-composer-archive/export-bigquery.csv"],
    dag=dag,
)

archive_file = GCSToGCSOperator(
    task_id="archive_gcs_file",
    source_bucket=BUCKET_NAME,
    source_object=FILE_OBJ,
    destination_bucket=BUCKET_NAME_ARCHIVE,
    destination_object="backup_" + FILE_OBJ,
)

delete_table = BigQueryDeleteTableOperator(
    task_id="delete_table",
    deletion_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}",
    dag=dag,

)

end = PythonOperator(
        task_id='end',
        python_callable=print_string,
        op_kwargs={'to_print': 'END'},
    )


start >> load_csv >> save_sql_query >> [
    delete_table, archive_file, bigquery_to_gcs] >> end
