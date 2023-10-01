from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import datetime
from datetime import timedelta
from remote_api_calls.WeatherAPI import get_weather_data

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
args = {
    'owner': 'airflow_local',
    'depends_on_past': False,
    'start_date': datetime(year=2023, day=1, month=9, hour=0, minute=0, second=0),
    'email': ['manthapavankumar11@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    dag_id='weather_dag_1',
    default_args=args,
    description='calls weather apis and create csv data file',
    schedule=timedelta(minutes=1)
)

task_api_call = PythonOperator(
    task_id='call_api_task_1',
    depends_on_past=False,
    python_callable=get_weather_data,
    op_kwargs={'city_name': 'Hyderabad, Telangana, India'},
    dag=dag
)

end_task = BashOperator(
    task_id='end_task',
    depends_on_past=False,
    bash_command='echo workflow finished',
    dag=dag
)

task_api_call >> end_task
