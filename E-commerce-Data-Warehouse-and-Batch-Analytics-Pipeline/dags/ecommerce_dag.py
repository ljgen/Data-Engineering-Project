from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from ecommerce_data_prep import extract_data, transform_data, load_to_bigquery


# initialization
default_args = {
    'owner': 'tipakorn',
    'start_date': days_ago(0),
}

# Define the DAG
with DAG(
    'ecommerce_data_etl',
    default_args=default_args,
    description='ETL process for E-commerce Data Warehouse & Batch Analytics Pipeline',
    schedule_interval='1 0 * * *',  # Run at 12:01 AM every day
    catchup=False,
    max_active_runs=1
) as dag:

    # Define tasks
    extract_data_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True  # To pass XCom context
    )

    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True  # To pass XCom context
    )

    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_to_bigquery,
        provide_context=True  # To pass XCom context
    )

    # Set task dependencies
    extract_data_task >> transform_data_task >> load_data_task