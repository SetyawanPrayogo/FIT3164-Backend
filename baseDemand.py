import pandas as pd
import numpy as np
import ast
from dataTransformation import clean_sell_prices, get_start_end_year

from tqdm.auto import tqdm
tqdm.pandas()

__author__ = "Hamid Abrar Mahir (32226136), Setyawan Prayogo (32213816), Yuan She (32678304), Regina Lim (32023863)"

def main():   
    # Read calendar dataset
    calendar = pd.read_csv("dataset/calendar.csv")
    calendar['date'] = pd.to_datetime(calendar['date'])
    calendar = calendar[calendar['date'] <= '2016-05-22']
    
    newCalendar = cleanCalendar(calendar)  
    
    # Read sales_train dataset
    sales_train = pd.read_csv("dataset/sales_train_evaluation.csv")
    sales_train['id'] = sales_train['id'].str.replace('_evaluation', '') # Remove evaluation in 'id' column
    
    weeklySalesTrain = weeklySales(sales_train, newCalendar)
    
    sell_prices = pd.read_csv("dataset/sell_prices.csv")
    sell_prices = clean_sell_prices(sell_prices, calendar.groupby('wm_yr_wk').agg({'date': 'min', 'd': 'first'}).reset_index())
    
    demandDF = sell_prices.copy(deep=True)
    demandDF['withoutBoth'] = demandDF.progress_apply(lambda row: createSummary(row, sales_train, calendar, newCalendar, False, False), axis=1)
    demandDF['withBoth'] = demandDF.progress_apply(lambda row: createSummary(row, sales_train, calendar, newCalendar, True, True), axis=1)
    demandDF['onlyEvent'] = demandDF.progress_apply(lambda row: createSummary(row, sales_train, calendar, newCalendar, True, False), axis=1)
    demandDF['onlySNAP'] = demandDF.progress_apply(lambda row: createSummary(row, sales_train, calendar, newCalendar, False, True), axis=1)

    return newCalendar, weeklySalesTrain, demandDF

def cleanCalendar(calendar):
    start_date = calendar.groupby('wm_yr_wk').agg({'date': 'min', 'd': 'first'}).reset_index() # Get the value of start date and end date of every wm_yr_wk
    
    newCalendar = pd.DataFrame({'wm_yr_wk': calendar['wm_yr_wk'].unique()})
    newCalendar = newCalendar[newCalendar['wm_yr_wk'] <= 11617]
    newCalendar = newCalendar.merge(start_date, on='wm_yr_wk', how='left')
    newCalendar = newCalendar.rename(columns={'date': 'start_date', 'd':'start_d'})
    
    newCalendar['end_date'] = newCalendar['start_date'] + pd.DateOffset(days=6)
    newCalendar.loc[newCalendar['wm_yr_wk'] == 11617, 'end_date'] = pd.to_datetime('2016-05-22')

    newCalendar['start_d_num'] = newCalendar['start_d'].str.extract(r'(\d+)').astype(int)
    newCalendar['end_d_num'] = newCalendar['start_d_num'] + 6
    newCalendar.loc[newCalendar['wm_yr_wk'] == 11617, 'end_d_num'] = 1941
    newCalendar['end_d'] = 'd_' + newCalendar['end_d_num'].astype(int).astype(str)
        
    for wm_yr_wk in newCalendar['wm_yr_wk'].unique():
        filtered_calendar = calendar[calendar['wm_yr_wk'] == wm_yr_wk]
        
        event_type_1_values = filtered_calendar['event_type_1'].dropna()
        event_type_2_values = filtered_calendar['event_type_2'].dropna()
        
        event_type = event_type_1_values._append(event_type_2_values) # Same as concat
        event_num = event_type.count()
        event_type = event_type.value_counts().to_dict()
        
        newCalendar.loc[newCalendar['wm_yr_wk'] == wm_yr_wk, 'event_num'] = event_num
        newCalendar.loc[newCalendar['wm_yr_wk'] == wm_yr_wk, 'event_type'] = str(event_type)
    
    newCalendar['event_num'] = newCalendar['event_num'].astype(int)
    newCalendar['event_type'] = newCalendar['event_type'].apply(ast.literal_eval)
    
    snap_CA_count = calendar.groupby('wm_yr_wk')['snap_CA'].sum().reset_index()
    snap_TX_count = calendar.groupby('wm_yr_wk')['snap_TX'].sum().reset_index()
    snap_WI_count = calendar.groupby('wm_yr_wk')['snap_WI'].sum().reset_index()
    
    newCalendar = newCalendar.merge(snap_CA_count, on='wm_yr_wk', how='left')
    newCalendar = newCalendar.merge(snap_TX_count, on='wm_yr_wk', how='left')
    newCalendar = newCalendar.merge(snap_WI_count, on='wm_yr_wk', how='left')
    
    newCalendar = newCalendar[['wm_yr_wk','start_date', 'end_date', 'start_d', 'end_d', 'event_num', 'event_type',
                               'snap_CA', 'snap_TX', 'snap_WI']]
    
    return newCalendar

def weeklySales(sales_train, newCalendar):
    sales_day = sales_train.iloc[:, 6:]
    
    rolling_mean_df= sales_day.T.rolling(window=7).sum().iloc[6::7, :].T
    last_2_column = sales_day.T.rolling(window=2).sum().iloc[-1:, :].T
    weekly_sales_train = pd.concat([sales_train.iloc[:, :6], rolling_mean_df, last_2_column], axis=1)
    
    for _, row in newCalendar.iterrows():
        wm_yr_wk = row['wm_yr_wk']
        end_d = row['end_d']
        weekly_sales_train.rename(columns={end_d: wm_yr_wk}, inplace=True)
        
    return weekly_sales_train

def keyWeek(calendar, state_id, event: bool, snap: bool):
    snapFilter = f"snap_{state_id}" 
    
    if event == False and snap == False:
        filteredCalendar = calendar[(calendar['event_num'] == 0) & (calendar[snapFilter] == 0)]
    elif event == True and snap == False:
        filteredCalendar = calendar[(calendar['event_num'] != 0) & (calendar[snapFilter] == 0)]
    elif event == False and snap == True:
        filteredCalendar = calendar[(calendar['event_num'] == 0) & (calendar[snapFilter] != 0)]
    elif event == True and snap == True:
        filteredCalendar = calendar[(calendar['event_num'] != 0) & (calendar[snapFilter] != 0)]
        
    return filteredCalendar['wm_yr_wk'].unique()

def createSummary(row, sales_train, calendar, newCalendar, event: bool, snap: bool):
    key = row['item_id'] + '_' + row['store_id']
    state_id = row['state_id']
    
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
    
    wm_yr_wk_key = keyWeek(newCalendar, state_id, event, snap)
    d_key = calendar[calendar['wm_yr_wk'].isin(wm_yr_wk_key)]['d'].unique()
    
    filtered_sales = sales_train[sales_train['id'] == key]
    filtered_sales = filtered_sales[['id', 'item_id', 'dept_id', 'cat_id', 'store_id', 'state_id'] + [key for key in d_key]]
    
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
        
        start_d_num = int(start_d.split('_')[1])
        end_d_num = int(end_d.split('_')[1])
        
        day_n = filtered_sales.iloc[:, 6:].columns.str.split('_').str[1].astype(int)
        filtered_d_num = day_n[(day_n >= start_d_num) & (day_n <= end_d_num)]
        columnFilter = 'd_' + filtered_d_num.astype(str)
        
        num_weeks = len(filtered_d_num)/7
        
        summary.setdefault(year, {})['num_week'] = num_weeks # To ensure that the key is created if it doesn't exist
        
        total_sold = np.sum(filtered_sales[columnFilter].values)
        summary[year]['total_sold'] = total_sold
        
        if num_weeks == 0:
            # Handle the case where num_weeks is 0
            summary[year]['avg_sold_wk'] = 0
            summary[year]['avg_rev_wk'] = 0
        else:
            avg_sold_wk = round(total_sold/num_weeks, 3)
            avg_rev_wk = round(avg_sold_wk*row['sell_price'], 3)

            summary[year]['avg_sold_wk'] = avg_sold_wk 
            summary[year]['avg_rev_wk'] = avg_rev_wk
        
        summary[year]['avg_event_wk'] = round(newCalendar[newCalendar['wm_yr_wk'].isin(wm_yr_wk_key)]['event_num'].mean(), 3)
        
        snapFilter = f"snap_{state_id}" 
        summary[year]['avg_snap'] = round(newCalendar[newCalendar['wm_yr_wk'].isin(wm_yr_wk_key)][snapFilter].mean(), 3)
        
        # Increment indicator
        ind += 1
    # print(summary)
    return summary

if __name__ == '__main__':
    newCalendar, weeklySalesTrain, demandDF = main()
    newCalendar.to_csv("newCalendar.csv", index=False)
    
    demandDF.to_csv("demand.csv", index=False)
    
    # test = demandDF.iloc[0:100, :].progress_apply(lambda row: createSummary(row, sales_train, calendar, newCalendar, True, False), axis=1)
    # test[0]
    # demandDF.iloc[0,:]['summary']['withoutBoth']