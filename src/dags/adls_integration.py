from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook
from datetime import datetime
from airflow.decorators import task, dag


@dag(dag_id="adls_integration", schedule=None, start_date=datetime(2023, 10, 26), catchup=False)
def adls_gen2_integration():
    @task()
    def extract():
        blob_connection = AzureDataLakeStorageV2Hook(adls_conn_id="azure_data_lake_default")
        data = blob_connection.list_file_system()
        return data

    data = extract()
    print(data)


adls_gen2_integration()
