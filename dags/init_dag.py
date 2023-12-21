import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from etl_scripts.init_db import initialize_database


with DAG(
    dag_id="init_dag", start_date=datetime(2023, 11, 20), schedule_interval="@once"
) as dag:

    database_init = PythonOperator(
        task_id = "database_init",
        python_callable = initialize_database,
        op_kwargs = {
            "conn_string": os.getenv("DATABASE_CONNECTION_STRING")
        }
    )

    database_init