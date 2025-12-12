from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

# definicion del dag
with DAG(
    dag_id="elt_weather_staging",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    template_searchpath="/opt/airflow"
) as dag:

# esta tarea ejecuta el sql de transformacion dentro de la data werehouse o sea postgresql
    transform_weather = PostgresOperator(
        task_id="transform_weather_staging",
        postgres_conn_id="postgres_dw",
        sql="sql/staging/transform_weather.sql"
    )
