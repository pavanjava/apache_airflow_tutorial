import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
args = {
    'owner': 'airflow_local',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['manthapavankumar11@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    dag_id='first_code',
    default_args=args,
    description='first simple DAG describing bash operator',
    schedule=timedelta(days=1)
)

task1 = BashOperator(
    task_id='task_1',
    bash_command='date',
    dag=dag
)

task1.doc_md = """\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
"""

dag.doc_md = __doc__

task2 = BashOperator(
    task_id='task_2',
    depends_on_past=False,
    bash_command='sleep 5',
    dag=dag
)

templated_command = """
    {% for i in range(5) %}
        echo "{{ds}}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""

task3 = BashOperator(
    task_id='templated_task',
    depends_on_past=False,
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

task1 >> [task2, task3]

