from urllib import request
import pendulum
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def _get_data(year, month, day, hour, output_path):
    print(day)
    day = str(int(day)-1)
    print(day)
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    # url = (
    #     "https://dumps.wikimedia.org/other/pageviews/"
    #     f"2025/2025-{month:0>2}/"
    #     f"pageviews-2025{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    # )
    
    # output_path = Path(output_path)
    # output_path.parent.mkdir(parents=True, exist_ok=True)
    request.urlretrieve(url, output_path)

def _fetch_pageviews(pagenames, logical_date):
    result = dict.fromkeys(pagenames, 0)

    logical_date = pendulum.parse(logical_date)
    year = logical_date.year
    month = logical_date.month
    day = logical_date.day
    hour = logical_date.hour

    logical_date = f'{hour}-{day}/{month}/{year}'

    with open("/home/dima/Airflow/tmp/wikipageviews", "r", encoding="utf-8") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts

    with open("/home/dima/Airflow/tmp/postgres_query.sql", "w", encoding="utf-8") as f:
        for pagename, pageviewcount in result.items():
            f.write(
                "INSERT INTO pageview_counts VALUES ("
                f"'{pagename}', {pageviewcount}, '{logical_date}'"
                ");\n"
            )


def _write_to_postgres(**context):
    logical_date = context["logical_date"]
    
    # Прочитайте данные из файла или передайте через XCom
    result = {
        "Google": 712,
        "Amazon": 9,
        "Apple": 54,
        "Microsoft": 173,
        "Facebook": 375
    }
    
    hook = PostgresHook(postgres_conn_id="my_postgres")
    
    for pagename, count in result.items():
        hook.run(
            "INSERT INTO pageview_counts VALUES (%s, %s, %s);",
            parameters=(pagename, count, logical_date)
        )


with DAG(
    dag_id="wiki_2",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    template_searchpath="/home/dima/Airflow/tmp",
    tags=["example"],
) as dag:

    get_data = PythonOperator(
        task_id="get_data",
        python_callable=_get_data,
        op_kwargs={
            "year": "{{ logical_date.year }}",
            "month": "{{ logical_date.month }}",
            "day": "{{ logical_date.day }}",
            "hour": "{{ logical_date.hour }}",
            "output_path": "/home/dima/Airflow/tmp/wikipageviews.gz",
        },
    )

    extract_gz = BashOperator(
        task_id="extract_gz",
        bash_command="gunzip --force /home/dima/Airflow/tmp/wikipageviews.gz",
    )

    fetch_pageviews = PythonOperator(
        task_id="fetch_pageviews",
        python_callable=_fetch_pageviews,
        op_kwargs={
            "pagenames": {"Google", "Amazon", "Apple", "Microsoft", "Facebook"},
            "logical_date": "{{ logical_date }}",
        },
    )

    write_to_postgres = PythonOperator(
        task_id="write_to_postgres",
        python_callable=_write_to_postgres,
    )

    get_data >> extract_gz >> fetch_pageviews >> write_to_postgres