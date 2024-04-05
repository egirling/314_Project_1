import pandas as pd
import pytest
from dagster import MetadataValue, MaterializeResult, AssetExecutionContext

from src import assets


# def test_remove_NA():
#     data = pd.DataFrame({"Destination": ["TRAPPIST-1e", "TRAPPIST-1e", "", "", "55 Cancri e"], 
#                          "Name": ["", "Solam Susent", "Reney Baketton", "", "Mollen Mcfaddennon"]})
#     actual = assets.remove_NA(data)
#     pd.testing.assert_frame_equal(
#         actual,
#         pd.DataFrame({"Destination": ["TRAPPIST-1e", "TRAPPIST-1e", "", "", "55 Cancri e"], 
#                       "Name": ["", "Solam Susent", "Reney Baketton", "", "Mollen Mcfaddennon"]}),
#     )

# test above not done, will finalize and rename stuff later

def test_splitWomanAndChildrenFromMen():
    data = pd.DataFrame({"Age": [36.0, 18.0, 8.0, 54.0, 11.0], "Transported": [False, True, False, True, True]})
    actual = assets.splitWomanAndChildrenFromMen(data)
    
    survival_rate_adults = float(1/2)
    survival_rate_children = float(1/3)
    pd.testing.assert_frame_equal(
        actual,
        MaterializeResult(
            metadata = {
                "Survival Rate of Adults": survival_rate_adults, 
                "Survival Rate of Children": survival_rate_children,
            }
        )
    )
    # expected = MaterializeResult(
    #     metadata = {
    #         "Survival Rate of Adults": survival_rate_adults, 
    #         "Survival Rate of Children": survival_rate_children,
    #     }
    # )
    # assert actual == expected