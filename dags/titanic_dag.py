import os
from datetime import datetime, timedelta

from airflow import DAG

# from airflow.operators.bash_operator import BashOperator

default_args = {
    "owner": "batch",
    "depends_on_past": False,
    "start_date": datetime(2020, 10, 10),
    "email": ["test@test.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="batch_dag",
    default_args=default_args,
    catchup=False,
    schedule_interval="@once",
)

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
pyspark_app_home = "/home"
