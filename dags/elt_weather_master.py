from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# dag orquestador de todos los otros dags
with DAG(
    dag_id="elt_weather_master",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False
) as dag:

# task que dispara el dag que extrae los datos desde la api
    extract = TriggerDagRunOperator(
        task_id="extract_weather",
        trigger_dag_id="elt_weather"
    )
# este task dispara el dag que carga los datos crudos en el raw
    load_raw = TriggerDagRunOperator(
        task_id="load_raw_weather",
        trigger_dag_id="elt_weather_load"
    )
# este task dispara el dag que transforma datos a staging
    staging = TriggerDagRunOperator(
        task_id="transform_staging",
        trigger_dag_id="elt_weather_staging"
    )
# este tastk dispara el dag que construye el mart (dim y fact)
    mart = TriggerDagRunOperator(
        task_id="build_mart",
        trigger_dag_id="elt_weather_mart"
    )
# este tast dispara el dag de control de calidad de datos
    quality = TriggerDagRunOperator(
        task_id="data_quality",
        trigger_dag_id="elt_weather_quality"
    )
# orden de ejecusion de los dags
# extraemos, cargamos, transformamos, luego mar y al final la calidad
    extract >> load_raw >> staging >> mart >> quality

#elt_weather_master se encarga de ejecutar todo el pipeline ELT en orden, desde la extraccion hasta la validacion de calidad