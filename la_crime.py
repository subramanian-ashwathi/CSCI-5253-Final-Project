from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"

###############################################
# DAG Definition
###############################################
now = datetime.now()
start = now - timedelta(days=1)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(start.year, start.month, start.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        dag_id="la_city_crime", 
        description="This DAG runs a Pyspark app that uses modules.",
        default_args=default_args, 
        schedule_interval=relativedelta(months=1),
    )

start = DummyOperator(task_id="start", dag=dag)

env_vars = {"EXECUTION_DATE": '{{ execution_date }}'}

spark_job = SparkSubmitOperator(
    task_id="spark_job",
    application="/usr/local/spark/app/lacity_crime.py", # Spark application path created in airflow and spark cluster
    name="hello-world-module",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    env_vars={"EXECUTION_DATE": '{{ execution_date }}'},
    dag=dag)

end = DummyOperator(task_id="end", dag=dag)

start >> spark_job >> end