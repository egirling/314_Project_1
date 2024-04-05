import pandas as pd

from dagster import MetadataValue, asset, MaterializeResult, AssetExecutionContext # import the `dagster` library
data = pd.read_csv("spaceship-titanic/train.csv")


@asset
def splitCabin(context: AssetExecutionContext) -> MaterializeResult:
   data[['Deck', 'Num', 'Side']] = data['Cabin'].str.split('/', expand=True)

   side_p = data[data['Side'] == 'P']
   side_s = data[data['Side'] == 'S']

   num_survived_side_p = side_p['Transported'].sum()
   num_survived_side_s = side_s['Transported'].sum()

   total_port_passengers = len(side_p)
   total_starboard_passengers = len(side_s)

   survival_rate_port = float(num_survived_side_p / total_port_passengers)
   survival_rate_starboard = float(num_survived_side_s / total_starboard_passengers)
  
   return MaterializeResult(
       metadata={
           "Survival Rate of Passengers Staying Port": survival_rate_port,
           "Survival Rate of Passengers Staying Starboard": survival_rate_starboard,
       }
   )


@asset 
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

@asset 
def sumAmenityCharges(context: AssetExecutionContext) -> MaterializeResult:
    data['money spent on amenities'] = data[['RoomService', 'FoodCourt', 'ShoppingMall', 'Spa', 'VRDeck']].sum(axis=1)

    data.drop(columns=['RoomService', 'FoodCourt', 'ShoppingMall', 'Spa', 'VRDeck'], inplace=True)

    threshold = data['money spent on amenities'].quantile(0.5)
    data['spender_category'] = data['money spent on amenities'].apply(lambda x: 'High Spender' if x >= threshold else 'Low Spender')
    #survival_rate_by_spender_category = data.groupby('spender_category')['Transported'].mean()
    survival_rate_high_spenders = float(data[data['money spent on amenities'] >= threshold]['Transported'].mean())
    survival_rate_low_spenders = float(data[data['money spent on amenities'] < threshold]['Transported'].mean())

    return MaterializeResult(
        metadata={
            "Survival Rate for High Spenders": survival_rate_high_spenders, 
            "Survival Rate for Low Spenders": survival_rate_low_spenders, 
        }
    )

@asset 
def splitByVIP(context: AssetExecutionContext) -> MaterializeResult:
    vip = data[data['VIP'] == True]
    non_vip = data[data['VIP'] == False]


    num_survived_vip = vip['Transported'].sum()
    num_survived_non_vip = non_vip['Transported'].sum()

    total_vip_passengers = len(vip)
    total_non_vip_passengers = len(non_vip)

    survival_rate_vip = float(num_survived_vip / total_vip_passengers)
    survival_rate_non_vip = float(num_survived_non_vip / total_non_vip_passengers)

    return MaterializeResult(
        metadata={
            "Survival Rate for VIP": survival_rate_vip, 
            "Survival Rate for Non-VIP": survival_rate_non_vip, 
        }
    )