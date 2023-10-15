import json
from datetime import datetime

from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import task, dag

from utils.data_transformer import transform


@dag(dag_id="products_etl_workflow",
     schedule=None,
     start_date=datetime(2023, 10, 14), catchup=False)
def products_etl_workflow():
    @task()
    def load_to_mongo(records):
        hook = MongoHook(conn_id="mongo_local")
        conn = hook.get_conn()
        db = conn.northwind
        collection = db.products
        print(records)
        records = json.loads(records)
        for record in records:
            collection.insert_one(record)

    extract = PostgresOperator(
        task_id="fetch_data_from_northwind",
        postgres_conn_id="postgresql-local-northwind",
        sql="select p.product_id,"
            "p.product_name,"
            "p.quantity_per_unit,"
            "p.unit_price,"
            "p.units_in_stock,"
            "p.units_on_order,"
            "c.category_id,"
            "c.category_name,"
            "c.description "
            "from products p, categories c "
            "where p.category_id = c.category_id",
        do_xcom_push=True
    )

    data_transform = PythonOperator(
        task_id='convert_to_json',
        python_callable=transform
    )

    extract >> data_transform >> load_to_mongo(data_transform.output)


products_etl_workflow()
