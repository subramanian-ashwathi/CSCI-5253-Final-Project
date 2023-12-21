from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from etl_scripts.transform import transform_data
from etl_scripts.load import load_data
from etl_scripts.extract import extract_data

airflow_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
csv_target_dir = airflow_home + "/data/{{ ds }}/downloads"
csv_target_file = csv_target_dir + "/outcomes_{{ ds }}.csv"

pq_target_dir = airflow_home + "/data/{{ ds }}/processed"

DATABASE_CONNECTION_STRING = os.getenv("DATABASE_CONNECTION_STRING")
SPARK_MASTER = "local[4]"

with DAG(
    dag_id="la_crime_dag", start_date=datetime(2023, 11, 1), schedule_interval="@monthly"
) as dag:

    extract = PythonOperator(
        task_id = "extract",
        python_callable = extract_data,
        provide_context=True,
        op_kwargs = {
            "connection_string": DATABASE_CONNECTION_STRING
        }
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
        op_kwargs={
            "database_connection_string": DATABASE_CONNECTION_STRING,
            "spark_master": SPARK_MASTER,
        },
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_data,
    )

    (
        extract
        >> transform
        # >> load
    )