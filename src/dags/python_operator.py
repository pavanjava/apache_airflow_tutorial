import json

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import datetime
from datetime import timedelta
from utils.WeatherAPI import create_csv_file

city_name = "Hyderabad, Telangana, India",
limit = 1,
API_KEY = "<OPEN_WEATHER_API_KEY>"


args = {
    'owner': 'airflow_local',
    'depends_on_past': False,
    'start_date': datetime(year=2023, day=1, month=9, hour=0, minute=0, second=0),
    # 'email': ['manthapavankumar11@gmail.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
        dag_id='weather_api_dag',
        catchup=False,
        schedule_interval='@daily',
        default_args=args
) as dag:

    task_fetch_geocodes = SimpleHttpOperator(
        task_id='fetch_geo_codes',
        http_conn_id='open_weather_geo_codes',
        endpoint=f'direct?q=Hyderabad,Telangana&limit=1&appid={API_KEY}',
        method='GET',
        response_filter=lambda response: json.loads(response.content),
        log_response=True
    )

    task_fetch_weather = SimpleHttpOperator(
        task_id='fetch_weather',
        http_conn_id='open_weather_data',
        endpoint='weather?lat={{ti.xcom_pull(task_ids=["fetch_geo_codes"])[0][0].get("lat")}}&lon={{ti.xcom_pull('
                 'task_ids=["fetch_geo_codes"])[0][0].get("lon")}}&appid=f38c94b357eae713857038d2f1a912cc',
        method='GET',
        response_filter=lambda response: json.loads(response.content),
        log_response=True
    )

    task_convert_to_csv = PythonOperator(
        task_id='convert_to_csv',
        python_callable=create_csv_file
    )

    # task_fetch_geocodes >> task_fetch_weather >> task_convert_to_csv
