import pandas as pd
import ast
from dataTransformation import generate_price_count, clean_sell_prices, get_start_end_year

from tqdm.auto import tqdm
tqdm.pandas()

__author__ = "Hamid Abrar Mahir (32226136), Setyawan Prayogo (32213816), Yuan She (32678304), Regina Lim (32023863)"

def main():
    calendar = pd.read_csv("../dataset/calendar.csv")
    calendar['date'] = pd.to_datetime(calendar['date'])
    calendar = calendar[calendar['date'] <= '2016-05-22']
    
    demandDF = pd.read_csv("../demand.csv")
    demandDF['start_date'] = pd.to_datetime(demandDF['start_date'])
    demandDF['end_date'] = pd.to_datetime(demandDF['end_date'])
    
    demandDF['withoutBoth'] = demandDF['withoutBoth'].apply(ast.literal_eval)
    demandDF['withBoth'] = demandDF['withBoth'].apply(ast.literal_eval)
    demandDF['onlyEvent'] = demandDF['onlyEvent'].apply(ast.literal_eval)
    demandDF['onlySNAP'] = demandDF['onlySNAP'].apply(ast.literal_eval)
    
    sell_prices = pd.read_csv("../dataset/sell_prices.csv")
    sell_prices = clean_sell_prices(sell_prices, calendar.groupby('wm_yr_wk').agg({'date': 'min', 'd': 'first'}).reset_index())
    
    priceDF = sell_prices.copy(deep=True)
    priceDF = pd.DataFrame(priceDF[['state_id', 'store_id', 'cat_id', 'dept_id', 'item_id']].drop_duplicates().reset_index(drop=True))
    
    priceDF['basePrice_withoutBoth'] = priceDF.progress_apply(lambda row: generateBasePrice(row, sell_prices, demandDF, False, False), axis=1)
    priceDF['basePrice_withBoth'] = priceDF.progress_apply(lambda row: generateBasePrice(row, sell_prices, demandDF, True, True), axis=1)
    priceDF['basePrice_onlyEvent'] = priceDF.progress_apply(lambda row: generateBasePrice(row, sell_prices, demandDF, True, False), axis=1)
    priceDF['basePrice_onlySNAP'] = priceDF.progress_apply(lambda row: generateBasePrice(row, sell_prices, demandDF, False, True), axis=1)
    
    priceDF['Price Count'] = priceDF.progress_apply(generate_price_count, args=(sell_prices,), axis=1)
    
    return priceDF
    
def generateBasePrice(row, sell_prices, demandDF, event: bool, snap: bool):
    base_price = {year: [] for year in range(2011, 2017)} # Create dictionary from 2011 to 2016
    # print(row['item_id'])
    
    data = sell_prices[(sell_prices['store_id'] == row['store_id']) & (sell_prices['item_id'] == row['item_id'])]
    productSalesInfo = demandDF[(demandDF['store_id'] == row['store_id']) & (demandDF['item_id'] == row['item_id'])].reset_index()
    print(productSalesInfo)
    
    if event == False and snap == False:
        colName = 'withoutBoth'
    elif event == True and snap == False:
        colName = 'onlyEvent'
    elif event == False and snap == True:
        colName = 'onlySNAP'
    elif event == True and snap == True:
        colName = 'withBoth'
        
    # Put all of the price in each year
    for _, item in data.iterrows():
        start_year, end_year = get_start_end_year(item)
        
        curr_price = float(item['sell_price'])
        for year in range(start_year, end_year+1):
            base_price[year].append(curr_price)
    
    #print(base_price)
    # Get the base price
    for year, price in base_price.items():
        sorted_prices = sorted([p for p in price if p is not None], reverse=True)
        # print(sorted_prices, year)
        max_price = None
        if len(sorted_prices):
            num_week = []
            for prices in sorted_prices:
                # print(prices)
                price_row = productSalesInfo[(productSalesInfo['sell_price'] == prices) & (productSalesInfo['start_date'].dt.year <= year) & 
                                  (productSalesInfo['end_date'].dt.year >= year)].reset_index()
                # print(price_row)
                
                for i in range(0,len(price_row)):
                    num_week.append(price_row[colName][i][year]['num_week'])
                
                # print(prices, num_week)
                if has_non_zero_value(num_week):
                    max_price = prices
                    break
                
                if len(sorted_prices) == 1 and not has_non_zero_value(num_week):
                    max_price = 0
                    break
            if max_price is None:
                max_price = 0
        else:
            max_price = 0
        base_price[year] = max_price
    return base_price

def has_non_zero_value(arr):
    return any(val != 0 for val in arr)
    
if __name__ == '__main__':
    priceDF = main()
    
    priceDF.to_csv("../price.csv", index=False)
    
    # test = priceDF.iloc[0:10, :].progress_apply(lambda row: generateBasePrice(row, sell_prices, demandDF, False, False), axis=1)
    
    # priceDF.iloc[41:42, :].progress_apply(lambda row: generateBasePrice(row, sell_prices, demandDF, False, False), axis=1)
    
    # oldPrice = pd.read_csv("oldPrice.csv")
    # test.equals(oldPrice.iloc[0:10,:]['Base Price'])