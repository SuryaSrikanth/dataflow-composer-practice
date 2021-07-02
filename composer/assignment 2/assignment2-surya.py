import datetime

from airflow import models
from airflow.models import Variable
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


def print_string(to_print):
    print(to_print)
    return True


 # otherwise, type it like this

PROJECT_ID = 'sudarshan-training'
DATASET_NAME = 'composer_training'
BUCKET_NAME = 'gcp-training-composer'
BUCKET_NAME_DST = 'gcp-training-composer-archive'


dag = models.DAG(
    dag_id='assignment2-surya',
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['example'],
)

dag.doc_md = """
Assignment 2
""" 

start = PythonOperator(
    task_id='start',
    python_callable=print_string,
    op_kwargs={'to_print': 'START'},
)

load_csv = GCSToBigQueryOperator(
    task_id='gcs_to_bigquery',
    bucket=BUCKET_NAME,
    source_objects=["{{ dag_run.conf['name'] }}"],
    destination_project_dataset_table=DATASET_NAME+"." +
    '{{ var.json.get("variables_config")[dag_run.conf["name"]]["TABLE_NAME"] }}',
    autodetect=True,
    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows=1,
    dag=dag,
)

save_sql_query = BigQueryExecuteQueryOperator(
    task_id="save_sql_query",
    sql=str(
        '{{ var.json.get("variables_config")[dag_run.conf["name"]]["QUERY"] }}'),
    use_legacy_sql=False,
    destination_dataset_table=DATASET_NAME+"." +
    '{{ var.json.get("variables_config")[dag_run.conf["name"]]["TABLE"] }}',
    write_disposition='WRITE_TRUNCATE',
    dag=dag,

)

bigquery_to_gcs = BigQueryToGCSOperator(
    task_id="bigquery_to_gcs",
    source_project_dataset_table=DATASET_NAME+"." +
    '{{ var.json.get("variables_config")[dag_run.conf["name"]]["TABLE"] }}',
    destination_cloud_storage_uris=[
        '{{ var.json.get("variables_config")[dag_run.conf["name"]]["BIGQUERY_TO_GCS"] }}'],
    dag=dag,
)

archive_file = GCSToGCSOperator(
    task_id="archive_file",
    source_bucket=BUCKET_NAME,
    source_object="{{ dag_run.conf['name'] }}",
    destination_bucket=BUCKET_NAME_DST,
    destination_object='{{ var.json.get("variables_config")[dag_run.conf["name"]]["ARCHIVE"] }}' + \
    "{{ dag_run.conf['name'] }}",
)

delete_table = BigQueryDeleteTableOperator(
    task_id="delete_table",
    deletion_dataset_table=PROJECT_ID + "." + DATASET_NAME+"." +
    '{{ var.json.get("variables_config")[dag_run.conf["name"]]["TABLE_NAME"] }}',
    dag=dag,
)

end = PythonOperator(
    task_id='end',
    python_callable=print_string,
    op_kwargs={'to_print': 'END'},
)

start >> load_csv >> save_sql_query >> [
    delete_table, archive_file, bigquery_to_gcs] >> end



# https://accounts.google.com/o/oauth2/v2/auth?client_id=881791521301-92i7ni0mknhtdepciluvc3rtrf249tji.apps.googleusercontent.com&response_type=code&scope=openid+email&redirect_uri=https://iap.googleapis.com/v1/oauth/clientIds/881791521301-92i7ni0mknhtdepciluvc3rtrf249tji.apps.googleusercontent.com:handleRedirect&code_challenge=Ctz0gQTw4Vy304gKrLKc7qvBYlvFNCzSC7oErzrHaYg&code_challenge_method=S256&cred_ref=true&state=eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InVYOGFfUSJ9.eyJyZnAiOiJUamF4SkJVMjU1cjgzTTVBX3o4Q2VGN0R2RUVMSVRDRi1ReXByUjVvM2lVIiwiaXNzIjoiaHR0cHM6Ly9jbG91ZC5nb29nbGUuY29tL2lhcCIsImF1ZCI6Ijg4MTc5MTUyMTMwMS05Mmk3bmkwbWtuaHRkZXBjaWx1dmMzcnRyZjI0OXRqaS5hcHBzLmdvb2dsZXVzZXJjb250ZW50LmNvbSIsInRhcmdldF91cmkiOiJodHRwczovL2VmMjJjMzI1NDVlOGQzNWVhcC10cC5hcHBzcG90LmNvbS8_Z2NwLWlhcC1tb2RlPUFVVEhFTlRJQ0FUSU5HIiwib3JpZ2luX3VyaSI6Imh0dHBzOi8vZWYyMmMzMjU0NWU4ZDM1ZWFwLXRwLmFwcHNwb3QuY29tLyIsImlhdCI6MTYyNTIzMDk1NSwiZXhwIjoxNjI1MjMxNTU1LCJlbmNyeXB0ZWRfY29kZV92ZXJpZmllciI6Ilx1MDAwMOBcdTAwMWWFmVx1MDAxOaZrrFx0fpVS88VcYjqpQl6pXHUwMDE3ZFxc2N5YP53aTpOgICnL7CSQelehuDVzvdHFqf-lU2t0XHUwMDFh73VdXHUwMDBl9TP_401cIoujYNr8Y2bxVLxiNqxcdTAwMWIvIn0.q-8bCNagU0SNcOMZhRmTHxzJBpvLN4PS9fMSJg8Yk2TcFZYARNrl2NNukRU8Upv0LLAg_d4O9CgT4TZ1liJtIA