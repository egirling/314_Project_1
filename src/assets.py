import pandas as pd
import matplotlib.pyplot as plt

from dagster import MetadataValue, asset, MaterializeResult, AssetExecutionContext # import the `dagster` library
data = pd.read_csv("spaceship-titanic/train.csv")

@asset
def remove_NA(context: AssetExecutionContext) -> MaterializeResult:
   rows_before_drop = data.shape[0]
   data.dropna(inplace=True)
   rows_after_drop = data.shape[0]

   return MaterializeResult(
       metadata={        
           "Rows Before Removing NA Values": rows_before_drop,
           "Rows After Removing NA Values": rows_after_drop,
        }
   )


@asset
def splitCabin(context: AssetExecutionContext) -> MaterializeResult:
   global data
   data[['Deck', 'Num', 'Side']] = data['Cabin'].str.split('/', expand=True)


   side_stats = data.groupby('Side').agg({'Transported': ['sum', 'count']})
   side_stats.columns = ['Survivors', 'TotalPassengers']
   side_stats.reset_index(inplace=True)

   plt.figure(figsize=(8, 6))

   bar_width = 0.35

   positions = range(len(side_stats))

   plt.bar(positions, side_stats['TotalPassengers'], bar_width, color='skyblue', label='Total Passengers')
   plt.bar([pos + bar_width for pos in positions], side_stats['Survivors'], bar_width, color='salmon', label='Survivors')

   plt.title('Number of Passengers and Survivors Across Sides')
   plt.xlabel('Side')
   plt.ylabel('Count')
   plt.xticks([pos + bar_width / 2 for pos in positions], side_stats['Side'])
   plt.legend()
   plt.tight_layout()
   plt.show()  

   deck_stats = data.groupby('Deck').agg({'Transported': ['sum', 'count']})
   deck_stats.columns = ['Survivors', 'TotalPassengers']
   deck_stats.reset_index(inplace=True)

   plt.figure(figsize=(12, 6))

   bar_width = 0.35

   positions = range(len(deck_stats))

   plt.bar(positions, deck_stats['TotalPassengers'], bar_width, color='skyblue', label='Total Passengers')
   plt.bar([pos + bar_width for pos in positions], deck_stats['Survivors'], bar_width, color='salmon', label='Survivors')

   plt.title('Number of Passengers and Survivors in Each Deck')
   plt.xlabel('Deck')
   plt.ylabel('Count')
   plt.xticks([pos + bar_width / 2 for pos in positions], deck_stats['Deck'])
   plt.legend()
   plt.tight_layout()
   plt.show()



   side_p = data[data['Side'] == 'P']
   side_s = data[data['Side'] == 'S']
   

   num_survived_side_p = side_p['Transported'].sum()
   num_survived_side_s = side_s['Transported'].sum()

   data = data.drop('Cabin', axis=1)

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

    plt.figure(figsize=(10, 6))

    bar_width = 0.35

    positions = [0, 1]

    plt.bar(positions, [total_adults, total_children], bar_width, color='skyblue', label='Total Passengers')
    plt.bar([pos + bar_width for pos in positions], [survived_adults, survived_children], bar_width, color='salmon', label='Survivors')

    plt.title('Number of Passengers and Survivors for Adults and Children')
    plt.xlabel('Passenger Type')
    plt.ylabel('Count')
    plt.xticks([pos + bar_width / 2 for pos in positions], ['Adults', 'Children'])
    plt.legend()
    plt.tight_layout()
    plt.show()

    
    
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

    plt.figure(figsize=(8, 6))

    plt.bar(['Total Passengers', 'Survivors'], [total_vip_passengers, num_survived_vip], color=['skyblue', 'salmon'])
    plt.title('Number of Passengers and Survivors for VIPs')
    plt.ylabel('Count')
    plt.show()

    plt.figure(figsize=(8, 6))

    plt.bar(['Total Passengers', 'Survivors'], [total_non_vip_passengers, num_survived_non_vip], color=['skyblue', 'salmon'])
    plt.title('Number of Passengers and Survivors for Non-VIPs')
    plt.ylabel('Count')
    plt.show()

    survival_rate_vip = float(num_survived_vip / total_vip_passengers)
    survival_rate_non_vip = float(num_survived_non_vip / total_non_vip_passengers)

    return MaterializeResult(
        metadata={
            "Survival Rate for VIP": survival_rate_vip, 
            "Survival Rate for Non-VIP": survival_rate_non_vip, 
        }
    )