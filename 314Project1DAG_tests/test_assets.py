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
#                       "Name": ["", "Solam Susent", "Reney Baketton", "", "Mollen Mcfaddennon"]})
#     )

# test above not done, will finalize and rename stuff later


def test_splitCabin():
    data = pd.DataFrame({"Cabin": ["F/4/S", "E/0/P", "B/0/S", "G/5/P"],
                         "Transported": [False, True, False, False]})
    actual = assets.splitCabin(data)

    survival_rate_port = float(0,5)
    survival_rate_starboard = float(0)

    pd.testing.assert_frame_equal(
        actual,
        MaterializeResult(
            metadata={
                "Survival Rate of Passengers Staying Port": survival_rate_port,
                "Survival Rate of Passengers Staying Starboard": survival_rate_starboard,
            }
        # pd.DataFrame({"Deck": ["F", "E", "B", "G"],
        #               "Cabin Number": [4, 0, 0, 5],
        #               "Side": ["S", "P", "S", "P"]})
        )
    )


def test_splitWomanAndChildrenFromMen():
    data = pd.DataFrame({"Age": [36.0, 18.0, 8.0, 54.0, 11.0], 
                         "Transported": [False, True, False, True, True]})
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


def test_sumAmenityCharges():
    data = pd.DataFrame({"RoomService": [1, 2, 3, 4],
                         "FoodCourt": [1, 2, 3, 4],
                         "ShoppingMall": [1, 2, 3, 4],
                         "Spa": [1, 2, 3, 4],
                         "VRDeck": [1, 2, 3, 4],
                         "Transported": [False, True, False, False]})
    actual = assets.sumAmenityCharges(data)

    survival_rate_high_spenders = float(0)
    survival_rate_low_spenders = float(0.5)

    pd.testing.assert_frame_equal(
        actual,
        MaterializeResult(
            metadata = {
                "Survival Rate for High Spenders": survival_rate_high_spenders, 
                "Survival Rate for Low Spenders": survival_rate_low_spenders,
            }
        )
        # pd.DataFrame({"Total Spent": [4, 8, 12, 16]})
    )


def test_splitByVIP():
    data = pd.DataFrame({"VIP": [True, False, False, True],
                         "Transported": [False, True, False, True]})
    actual = assets.splitByVIP(data)

    survival_rate_vip = float(1/2)
    survival_rate_non_vip = float(1/2)

    pd.testing.assert_frame_equal(
        actual,
        MaterializeResult(
            metadata = {
                "Survival Rate for VIP": survival_rate_vip, 
                "Survival Rate for Non-VIP": survival_rate_non_vip,
            }
        )
        # pd.DataFrame({"VIP": [True, False, False, True],
        #               "Not VIP": [False, True, True, False]})
    )