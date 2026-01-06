from datetime import datetime
from pathlib import Path
import pandas as pd

# Обновлённые импорты для Airflow 3.x
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup  # Рекомендуется для структурирования задач

# Инициализация DAG (без изменений синтаксиса, но с учётом новых конвенций)
with DAG(
    dag_id="01_unscheduled",
    start_date=datetime(2019, 1, 1),
    schedule=None,  # В Airflow 2.2+ и 3.x schedule_interval заменён на schedule
    tags=['educ'],  # Рекомендуется добавлять теги для организации
) as dag:

    # Задача 1: загрузка событий
    fetch_events = BashOperator(
        task_id="fetch_events",
        bash_command=(
            "mkdir -p /data/events && "
            "curl -o /data/events.json http://events_api:5000/events"
        ),
    )

    # Функция расчёта статистики (без изменений)
    def _calculate_stats(input_path, output_path):
        """Calculates event statistics."""
        Path(output_path).parent.mkdir(exist_ok=True)
        events = pd.read_json(input_path)
        stats = events.groupby(["date", "user"]).size().reset_index()
        stats.to_csv(output_path, index=False)

    # Задача 2: расчёт статистики
    calculate_stats = PythonOperator(
        task_id="calculate_stats",
        python_callable=_calculate_stats,
        op_kwargs={"input_path": "/data/events.json", "output_path": "/data/stats.csv"},
    )

    # Определение зависимостей (порядок выполнения задач)
    fetch_events >> calculate_stats