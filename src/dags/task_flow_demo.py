import pandas as pd
from datetime import datetime
from airflow.decorators import task, dag


@dag(dag_id="housing_etl_workflow", schedule=None, start_date=datetime(2023, 10, 7), catchup=False)
def city_population_etl_workflow():
    @task()
    def extract():
        df = pd.read_csv("/Users/pavanmantha/Desktop/IndianHouses.csv")
        return df

    @task()
    def transform(data):
        data = pd.DataFrame(data=data)
        filtered_property = data[data["Transaction"] == "New_Property"]
        return filtered_property

    @task()
    def load(property):
        print(property)

    df = extract()
    new_properties = transform(data=df)
    load(property=new_properties)


city_population_etl_workflow()
