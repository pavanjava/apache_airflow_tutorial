import pandas as pd
source_url = "https://airflowstorage001.blob.core.windows.net/generic-csv-container/IndianHouses.csv?sp=r&st=2023-10-06T15:57:13Z&se=2023-10-06T23:57:13Z&spr=https&sv=2022-11-02&sr=b&sig=9AdQ2jVPjQBwADgKo4BD%2BaYIWZ%2FAgCbPn%2BFvQR9QAss%3D"


data = pd.read_csv(source_url)
print(data[data["Transaction"] == "New_Property"])