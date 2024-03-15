import pandas as pd
import numpy as np

__author__ = "Hamid Abrar Mahir (32226136), Setyawan Prayogo (32213816), Yuan She (32678304), Regina Lim (32023863)"

def main():
    """
    Function description:
    :Output:

    """
    """
    Read datasets
    """
    # Read sales_train dataset
    sales_train = pd.read_csv("dataset/sales_train_evaluation.csv")
    sales_train['id'] = sales_train['id'].str.replace('_evaluation', '') # Remove evaluation in 'id' column
    
    # Read calendar dataset
    calendar = pd.read_csv("dataset/calendar.csv")
    calendar['date'] = pd.to_datetime(calendar['date'])
    start_date = calendar.groupby('wm_yr_wk').agg({'date': 'min', 'd': 'first'}).reset_index() # Get the value of start date and end date of every wm_yr_wk

    # Read sell_prices dataset
    sell_prices = pd.read_csv("dataset/sell_prices.csv")
    sell_prices = clean_sell_prices(sell_prices, start_date)
    
    """
    Create Final Dataset
    """
    final_df = sell_prices.copy(deep=True)
    final_df['Summary'] = final_df.apply(generate_summary, args=(sales_train,calendar,), axis=1)
    
    """
    Create Price Dataset and add Base Price for Each Product in each year
    """
    price = sell_prices.copy(deep=True)
    price = pd.DataFrame(price[['store_id', 'item_id']].drop_duplicates().reset_index(drop=True))
    price['Base Price'] = price.apply(generate_base_price, args=(sell_prices,), axis=1)
    
    """
    Add Price Count for Each Product in each year
    """
    price['Price Count'] = price.apply(generate_price_count, args=(sell_prices,), axis=1)
    
    return final_df, price

def clean_sell_prices(sell_prices, start_date):
    """
    Function description: clean sell prices dataset (add more later)
    :Input:
        sell_prices:
        start_date:
    :Output:
        sell_prices:
    """
    # Remove sell_prices row if it has the same price as the previous week (only keep the first time it change)
    sell_prices = sell_prices.sort_values(by=['store_id', 'item_id', 'wm_yr_wk'])
    sell_prices['price_change'] = sell_prices.groupby(['store_id', 'item_id'])['sell_price'].diff()  # Calculate sell_price difference differences
    sell_prices = sell_prices[(sell_prices['price_change'] != 0) | (sell_prices['price_change'].isna())] # Filter out
    sell_prices['price_change'] = sell_prices['price_change'].fillna(0) # Change NaN to 0
    
    # Drop any row that have wm_yr_wk more than 11617 (Since we only have the sales data until d_1941)
    sell_prices = sell_prices[sell_prices['wm_yr_wk'] <= 11617]
    
    # Edit for each year
    # sell_prices['price_change_count'] = sell_prices.groupby(['store_id', 'item_id'])['item_id'].transform('count') # Add price change count

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
    sell_prices = sell_prices[['store_id', 'item_id', 'start_date', 'end_date', 'start_d', 'end_d', 'sell_price', 'price_change']]
    return sell_prices

def get_start_end_year(row):
    """
    Function description: **Get start year and end year
    :Input:

    :Output:

    """
    return row['start_date'].year, row['end_date'].year  

def generate_summary(row, sales_train, calendar):
    """
    Function description:
    :Input:

    :Output:

    """
    key = row['item_id'] + '_' + row['store_id']
    # print(key)
    state = row['store_id'].split('_')[0]
    
    start_year, end_year = get_start_end_year(row)
    num_years = len(range(start_year, end_year + 1))
    
    # Create boundaries
    boundaries = {
        2011: {'start': 'd_1', 'end': 'd_337'},
        2012: {'start': 'd_338', 'end': 'd_703'},
        2013: {'start': 'd_704', 'end': 'd_1068'},
        2014: {'start': 'd_1069', 'end': 'd_1433'},
        2015: {'start': 'd_1434', 'end': 'd_1798'},
        2016: {'start': 'd_1799', 'end': 'd_1941'}
    }
    # Indicator for year
    ind = 1
    
    # Filter rows where 'id' column is equal to key
    filtered_sales = sales_train[sales_train['id'] == key]
    
    summary = {}
    for year in range(start_year, end_year + 1):
        # Generate start_d
        if ind == 1:
            start_d = row['start_d']
        else:
            start_d = boundaries[year]['start']
        
        # Generate end_d
        if ind == num_years:
            end_d = row['end_d']
        else:
            end_d = boundaries[year]['end']
        
        # Get start_d and end_d in number
        start_d_num = int(start_d.split('_')[1])
        end_d_num = int(end_d.split('_')[1])
        
        # Get number of weeks
        num_weeks = (end_d_num - start_d_num + 1)/7
        
        total_sold = np.sum(filtered_sales.loc[:, start_d:end_d].values)
        avg_sold_wk = round(total_sold/num_weeks, 3)
        avg_rev_wk = round(avg_sold_wk*row['sell_price'], 3)

        summary.setdefault(year, {})['avg_sold_wk'] = avg_sold_wk # To ensure that the key is created if it doesn't exist
        summary[year]['avg_rev_wk'] = avg_rev_wk

        # Filtered calendar for event and snap
        filtered_calendar = calendar.iloc[start_d_num-1:end_d_num]
        
        # Get event
        event_type_1_values = filtered_calendar['event_type_1'].dropna()
        event_type_2_values = filtered_calendar['event_type_2'].dropna()
        
        event = event_type_1_values._append(event_type_2_values) # Same as concat
        event = event.value_counts().to_dict()
        
        summary[year]['event'] = event

        # Get snap count
        if state == "CA":
            snap_count = filtered_calendar.loc[:, 'snap_CA'].sum()
        elif state == "TX":
            snap_count = filtered_calendar.loc[:, 'snap_TX'].sum()
        elif state == "WI":
            snap_count = filtered_calendar.loc[:, 'snap_WI'].sum()
        
        summary[year]['snap_count'] = snap_count
        
        # Increment indicator
        ind += 1
    # print(summary)
    return summary

def generate_base_price(row, sell_prices):
    """
    Function description: **Get base price on each year (max price on each year)
    :Input:

    :Output:

    """
    base_price = {year: [] for year in range(2011, 2017)} # Create dictionary from 2011 to 2016
    
    data = sell_prices[(sell_prices['store_id'] == row['store_id']) & (sell_prices['item_id'] == row['item_id'])]
    
    # Put all of the price in each year
    for _, item in data.iterrows():
        start_year, end_year = get_start_end_year(item)
        num_years = len(range(start_year, end_year + 1))
        
        curr_price = str(item['sell_price'])
        for year in range(start_year, end_year+1):
            base_price[year].append(curr_price)
    
    # Get the base price
    for year, price in base_price.items():
        max_price = max(price) if price else 0
        base_price[year] = float(max_price)
    
    return base_price

def generate_price_count(row, sell_prices):
    """
    Function description: **Get price count on each year
    :Input:

    :Output:

    """
    price_count = {year: 0 for year in range(2011, 2017)} # Create dictionary from 2011 to 2016
    
    data = sell_prices[(sell_prices['store_id'] == row['store_id']) & (sell_prices['item_id'] == row['item_id'])]

    for _, item in data.iterrows():
        start_year, end_year = get_start_end_year(item)
        
        # Increment count for each year between start_year and end_year
        for year in range(start_year, end_year + 1):
            if year in price_count:
                price_count[year] += 1
    
    return price_count

if __name__ == '__main__':
    final_df, price = main()
    print(final_df.shape) # (108547, 9)
    print(price.shape) # (30490, 4)
    final_df.to_csv("sales.csv", index=False)
    price.to_csv("price.csv", index=False)