import pandas as pd

__author__ = "Hamid Abrar Mahir (32226136), Setyawan Prayogo (32213816), Yuan She (32678304), Regina Lim (32023863)"

"""
sales_train.csv
"""
sales_train = pd.read_csv("dataset/sales_train_evaluation.csv")
# Remove evaluation in 'id' column
sales_train['id'] = sales_train['id'].str.replace('_evaluation', '')

"""
calendar.csv
"""
calendar = pd.read_csv("dataset/calendar.csv")
calendar['date'] = pd.to_datetime(calendar['date'])

# Get the value of start date and end date of every wm_yr_wk
start_date = calendar.groupby('wm_yr_wk').agg({'date': 'min', 'd': 'first'}).reset_index()

"""
sell_prices.csv
"""
sell_prices = pd.read_csv("dataset/sell_prices.csv")

# Remove sell_prices row if it has the same price as the previous week (only keep the first time it change)
sell_prices = sell_prices.sort_values(by=['store_id', 'item_id', 'wm_yr_wk'])
# Calculate sell_price difference differences
sell_prices['price_change'] = sell_prices.groupby(['store_id', 'item_id'])['sell_price'].diff()
# Filter out
sell_prices = sell_prices[(sell_prices['price_change'] != 0) | (sell_prices['price_change'].isna())]
# Change NaN to 0
sell_prices['price_change'] = sell_prices['price_change'].fillna(0)
# Add price change count
sell_prices['price_change_count'] = sell_prices.groupby(['store_id', 'item_id'])['item_id'].transform('count')

# Add start_date and start_d
sell_prices = sell_prices.merge(start_date, on='wm_yr_wk', how='left')
sell_prices = sell_prices.rename(columns={'date': 'start_date', 'd':'start_d'})
# Add end_date
sell_prices['end_date'] = sell_prices.groupby(['store_id', 'item_id'])['start_date'].shift(-1) - pd.DateOffset(days=1)
# For the last occurrence of each ID or IDs that only occur once, set end_date as '2016-05-22'
sell_prices.loc[sell_prices['end_date'].isna(), 'end_date'] = pd.to_datetime('2016-05-22')

# Add end_d
## Convert 'start_d' to integer by extracting the numeric part
sell_prices['start_d_num'] = sell_prices['start_d'].str.extract(r'(\d+)').astype(int)
sell_prices['end_d_num'] = sell_prices.groupby(['store_id', 'item_id'])['start_d_num'].shift(-1) - 1
# For the last occurrence of each ID or IDs that only occur once, set end_d as d_1941
sell_prices.loc[sell_prices['end_d_num'].isna(), 'end_d_num'] = 1941
# Bring back the format of "d_{number}"
sell_prices['end_d'] = 'd_' + sell_prices['end_d_num'].astype(int).astype(str)

# Reorder column and remove 'wm_yr_wk'
sell_prices = sell_prices[['store_id', 'item_id', 'start_date', 'end_date', 'start_d', 'end_d', 'sell_price', 'price_change', 'price_change_count']]


"""
Create Filtered Dataset (Final Dataset)
"""
filtered_df = sell_prices.copy(deep=True)

# Below can be done in array, made it per quarter 
# Add total product sold per quarter

# Add total revenue per quarter 

# Add event count during that period of price - in dictionary or array

# Add snap count

# if __name__ == '__main__':
    #sell_prices.to_csv("price.csv", index=False)
    