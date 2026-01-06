from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Используем контекст 'with DAG' (рекомендуемый способ в Airflow 2+/3.x)
with DAG(
    dag_id="wiki_dag",
    start_date=days_ago(3),
    schedule="@hourly",  # В Airflow 3.x schedule_interval → schedule
    tags=["educ"],  # Рекомендуется добавлять теги
) as dag:

    # Задача: загрузка данных из Wikimedia
    get_data = BashOperator(
        task_id="get_data",
        bash_command=(
            "curl -o /tmp/wikipageviews.gz "
            "https://dumps.wikimedia.org/other/pageviews/"
            "{{ execution_date.year }}/"
            "{{ execution_date.year }}-{{ '{:02}'.format(execution_date.month) }}/"
            "pageviews-{{ execution_date.year }}"
            "{{ '{:02}'.format(execution_date.month) }}"
            "{{ '{:02}'.format(execution_date.day) }}-"
            "{{ '{:02}'.format(execution_date.hour) }}0000.gz"
        ),
        # В Airflow 3.x параметр 'dag' больше не нужен внутри контекста 'with DAG'
    )

    # Явно определяем порядок выполнения (если есть другие задачи)
    # get_data >> ...  # Здесь можно добавить последующие задачи