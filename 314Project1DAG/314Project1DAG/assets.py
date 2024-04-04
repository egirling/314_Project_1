import pandas as pd

from dagster import asset # import the `dagster` library

@asset # add the asset decorator to tell Dagster this is an asset
def training_data() -> None:
    data = pd.read_csv("spaceship-titanic/train.csv")
    #return data
   
