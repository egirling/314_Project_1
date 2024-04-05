import pandas as pd
import pytest
from dagster import MetadataValue, MaterializeResult, AssetExecutionContext

from src import assets


def test_remove_NA():
    
    pass 


def test_splitWomanAndChildrenFromMen():
    data = pd.DataFrame({"Age": [36.0, 18.0, 8.0, 54.0, 11.0], "Transported": [False, True, False, True, True]})
    actual = assets.splitWomanAndChildrenFromMen(data)
    
    survival_rate_adults = float(1/2)
    survival_rate_children = float(1/3)
    expected = MaterializeResult(
        metadata = {
            "Survival Rate of Adults": survival_rate_adults, 
            "Survival Rate of Children": survival_rate_children,
        }
    )
    assert actual == expected