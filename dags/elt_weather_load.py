from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
from datetime import datetime
import json
import os

# funcion principal del dag
def load_weather_to_raw(**context):
    execution_date = context["ds"]
    # consstruimos el archivo json que genero el dag
    file_path = f"/opt/airflow/data/raw/weather/weather_{execution_date}.json"
    # validamos que el archivo exista
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"No existe el archivo RAW: {file_path}")

    # abrimos el json y cargamos como lista en python
    with open(file_path, "r") as f:
        data = json.load(f)

    # conexion a postgres usando airflow
    pg = PostgresHook(postgres_conn_id="postgres_dw")

    for record in data:
        city = record["city"]
        payload = json.dumps(record["payload"])

        insert_sql = """
            INSERT INTO raw_weather (payload, source_system, execution_date)
            VALUES (%s, %s, %s);
        """

    # ejecutamos la insercion en postgres
        pg.run(insert_sql, parameters=[payload, city, execution_date])

# definimos el dag
with DAG(
    dag_id="elt_weather_load",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    # definimos la tarea que ajusta la funcion
    load_weather_task = PythonOperator(
        task_id="load_raw_weather",
        python_callable=load_weather_to_raw,
        provide_context=True
    )

    load_weather_task
