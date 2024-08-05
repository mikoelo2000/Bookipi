from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bigquery_operator import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'weekly_churn_analysis_report',
    default_args=default_args,
    description='A DAG for generating weekly churn analysis report',
    schedule_interval='@weekly',  # Run every week
)

start = DummyOperator(task_id='start', dag=dag)

create_churn_report = BigQueryExecuteQueryOperator(
    task_id='create_churn_report',
    sql='CALL `BookipiProject.BookipiProduction.CreateWeeklyChurnReport`();',
    use_legacy_sql=False,
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

start >> create_churn_report >> end
