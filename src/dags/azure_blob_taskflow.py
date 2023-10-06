import json
import pandas as pd
from datetime import datetime
from airflow.decorators import task, dag
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook


@dag(dag_id="housing_etl_workflow", schedule=None, start_date=datetime(2023, 10, 6), catchup=False)
def city_population_etl_workflow():
    @task()
    def extract():
        container_connection = AzureDataLakeStorageV2Hook(adls_conn_id="azure-storage-acct")
        container_connection.test_connection()
        # data = container_connection.list_file_system()
        # return data

    # @task()
    # def transform(data: pd.DataFrame):
    #     filtered_property = data[data["Transaction"] == "New_Property"]
    #     return filtered_property
    #
    # @task()
    # def load(property: pd.DataFrame):
    #     print(property)

    df = extract()
    print(df)
    # new_properties = transform(data=df)
    # load(property=new_properties)


city_population_etl_workflow()
