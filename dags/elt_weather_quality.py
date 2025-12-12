from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException
from airflow.utils.dates import days_ago


# funcion para checkear la calidad de los datos de clima
def check_weather_quality():
    # conectamos a postgres usando la conexion configurada por aiflow
    pg = PostgresHook(postgres_conn_id="postgres_dw")

# reglas de calidad
    checks = {
        "Temperaturas fuera de rango": """
            SELECT COUNT(*) FROM fact_weather_readings
            WHERE temp_celsius < -50 OR temp_celsius > 60
        """,
        "Fechas nulas": """
            SELECT COUNT(*) FROM fact_weather_readings
            WHERE weather_timestamp IS NULL
        """,
        "City ID nulo": """
            SELECT COUNT(*) FROM fact_weather_readings
            WHERE city_id IS NULL
        """
    }
# recorre cada regla, ejecuta el sql y revisa si hay datos invalidos
    for rule, sql in checks.items():
        result = pg.get_first(sql)[0]
        # si hay registros invalidos o falla el dag para cortar el pipelina
        if result > 0:
            raise AirflowException(f"Falla de calidad: {rule}")


with DAG(
    dag_id="elt_weather_quality",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False
) as dag:

# task que ejecuta la funcion de control de calidad
    quality_check = PythonOperator(
        task_id="check_weather_data_quality",
        python_callable=check_weather_quality
    )

    quality_check
