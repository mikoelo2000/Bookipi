from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

def run_pyspark_job():
    import subprocess
    subprocess.run(['spark-submit', 'gs://Bookipi_Bucket/ BillingPeriod_Transformation.py'])

def call_bigquery_stored_procedure():
    client = bigquery.Client()
    procedure = 'CALL `BookipiProject.BookipiProduction.TransferAndDeduplicate_Subscription_Payments`();'
    query_job = client.query(procedure)
    query_job.result()

dag = DAG('data_pipeline', default_args=default_args, schedule_interval='@daily')

run_pyspark = PythonOperator(
    task_id='run_pyspark_job',
    python_callable=run_pyspark_job,
    dag=dag,
)

call_stored_procedures = PythonOperator(
    task_id='call_stored_procedures',
    python_callable=call_bigquery_stored_procedure,
    dag=dag,
)

run_pyspark >> call_stored_procedures
