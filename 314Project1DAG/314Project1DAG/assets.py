import json
import os

from dagster import asset # import the `dagster` library

@asset # add the asset decorator to tell Dagster this is an asset
def training_data() -> None:

    os.makedirs("trainting_data", exist_ok=True)
    with open("spaceship-titanic/train.csv", "w") as f:
        json.dump(training_data, f)
