# save as $AIRFLOW_HOME/dags/bakery_dag.py

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "you",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def make_bread(**kwargs):
    print("Making bread...")
    # return value -> will become an XCom automatically
    return "sourdough"
    # to simulate failure, uncomment this next line:
    # raise Exception("Oven broke!")

with DAG(
    dag_id="bakery_dag",
    default_args=default_args,
    description="Simple bakery example: open → make → pack",
    schedule_interval="0 2 * * *",
    start_date=datetime(2025, 8, 1),
    catchup=False,
    tags=["example", "bakery"],
) as dag:

    start = DummyOperator(task_id="bakery_open")

    make_bread_task = PythonOperator(
        task_id="make_bread",
        python_callable=make_bread,
    )

    pack_bread = BashOperator(
        task_id="pack_bread",
        bash_command='echo "Packing {{ ti.xcom_pull(task_ids=\'make_bread\') }}" && sleep 1'
    )

    start >> make_bread_task >> pack_bread
