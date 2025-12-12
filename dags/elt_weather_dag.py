from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
import requests
import json
import os
from datetime import datetime

# funcion p/ consumir api, guardar info de cada city, y guardar archivo json con todos los datos del dia
def extract_weather(**context):
    api_key = Variable.get("weather_api_key")
    cities = json.loads(Variable.get("weather_cities"))

    execution_date = context["ds"]

    raw_dir = "/opt/airflow/data/raw/weather"
    os.makedirs(raw_dir, exist_ok=True)

    all_data = []

    for city in cities:
        url = (
            f"https://api.openweathermap.org/data/2.5/weather?"
            f"q={city}&appid={api_key}&units=metric"
        )

        response = requests.get(url)
        payload = response.json()

        all_data.append({
            "city": city,
            "execution_date": execution_date,
            "payload": payload
        })

    # guardar archivo por fecha
    file_path = f"{raw_dir}/weather_{execution_date}.json"
    with open(file_path, "w") as f:
        json.dump(all_data, f)

# retornamos la ruta que apunta al archivo
    return file_path


with DAG(
    dag_id="elt_weather",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    extract_weather_task = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather,
        provide_context=True
    )

    extract_weather_task
