import pandas as pd

from dagster import MetadataValue, asset, MaterializeResult, AssetExecutionContext # import the `dagster` library
data = pd.read_csv("spaceship-titanic/train.csv")

@asset # add the asset decorator to tell Dagster this is an asset
def splitWomanAndChildrenFromMen(context: AssetExecutionContext) -> MaterializeResult:
    adults = data[data['Age'] > 18]
    children = data[data['Age'] <= 18]

    total_adults = len(adults)
    total_children = len(children)

    survived_adults = (adults['Transported'] == False).sum()  
    survived_children = (children['Transported'] == False).sum()  

    survival_rate_adults = float(survived_adults / total_adults if total_adults > 0 else 0)
    survival_rate_children = float(survived_children / total_children if total_children > 0 else 0)
    
    return MaterializeResult(
        metadata={
            "Survival Rate of Adults": survival_rate_adults, 
            "Survival Rate of Children": survival_rate_children,
        }
    )
    

