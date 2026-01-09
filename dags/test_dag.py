import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd


def _print_context(**context):
    # Пример: берём execution_date и dag_id
    # data = {
    #     "dag_id": [context["dag"].dag_id],
    #     "run_id": [context["run_id"]],
    #     "logical_date": [str(context["logical_date"])],
    # }

    # df = pd.DataFrame(context)
    # df.to_csv("~/Airflow/tmp/context.csv", index=False)
    print(context)


with DAG(
    dag_id="listing_4_03",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
) as dag:

    print_context = PythonOperator(
        task_id="print_context",
        python_callable=_print_context,
    )