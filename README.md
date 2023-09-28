# Apache Airflow Tutorial
this repository contains different concepts from airflow.

# Installing Airflow
go through below document to run the airflow locally.
- https://airflow.apache.org/docs/apache-airflow/2.1.0/start/local.html

[OR]

### Installation Instructions

- AIRFLOW_VERSION=2.1.0
- pip install "apache-airflow==${AIRFLOW_VERSION}"
- pip install 'apache-airflow[postgres]'

Note: we will use postgresql for our airflow backend.
Note: to point airflow to postgres we need to edit airflow.cfg. 
- navigate to airflow installation directory. 
- open airflow.cfg in your favaurate editor.
- search for `sql_alchemy_conn`
- assign `sql_alchemy_conn` to `postgresql+psycopg2://<db_user>:<db_pwd>@localhost/<db_schema>`

### Run Instructions

- airflow standalone
