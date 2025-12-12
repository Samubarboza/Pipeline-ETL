from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

# creamos un dag que define dos tareas que ejecutan sql en postgres
with DAG(
    dag_id="elt_weather_mart",
    start_date=days_ago(1),
    schedule_interval="@daily",
    template_searchpath="/opt/airflow/sql",
    catchup=False
) as dag:

# esto carga o actualiza la tabla dim_city usando los datos limpios del staging
    load_dim_city = PostgresOperator(
        task_id="load_dim_city",
        postgres_conn_id="postgres_dw",
        sql="mart/load_dim_city.sql"
    )
# y esto inserta las mediciones del clima en la tabla fact usando keys de dim_city
    load_fact_weather = PostgresOperator(
        task_id="load_fact_weather_readings",
        postgres_conn_id="postgres_dw",
        sql="mart/load_fact_weather.sql"
    )
# aca le decimos basicamente que ejecute fact despues de cargar la dimension
    load_dim_city >> load_fact_weather
