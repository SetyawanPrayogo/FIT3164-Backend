import pandas as pd
import ast

__author__ = "Hamid Abrar Mahir (32226136), Setyawan Prayogo (32213816), Yuan She (32678304), Regina Lim (32023863)"

def get_dataset(priceDF, salesDF, year):
    filtered_salesDF = salesDF[salesDF['Summary'].apply(lambda x: year in x)].reset_index()
    
    # Assign id
    filtered_salesDF['id'] = filtered_salesDF['item_id'] + '_' + filtered_salesDF['store_id']
    priceDF['id'] = priceDF['item_id'] + '_' + priceDF['store_id']
    
    group_id = filtered_salesDF.groupby(['id'])
    # print(len(filtered_salesDF))
    
    result_df = pd.DataFrame()
    for unique_id in filtered_salesDF['id'].unique():
        #print(unique_id)
        productSalesInfo = group_id.get_group((unique_id,))
        #print(productSalesInfo)
        
        productPriceInfo = priceDF[priceDF['id'] == unique_id].reset_index()
        #print(productPriceInfo)
        
        base_price = productPriceInfo['Base Price'][0][year]
        #print(base_price)
        
        base_price_row = productSalesInfo[productSalesInfo['sell_price'] == base_price].reset_index()

        if len(base_price_row) == 1:
            base_demand = base_price_row['Summary'][0][year]['avg_sold_wk']
        else:
            base_demand = base_price_row['Summary'][0][year]['avg_sold_wk']
            for i in range(1,len(base_price_row)):
                current_base_demand = base_price_row['Summary'][i][year]['avg_sold_wk']
                if current_base_demand < base_demand:
                    base_demand = current_base_demand
        
        # print(base_price, base_demand)
        
        # Iterate on every row of every unique_id
        for _, row in productSalesInfo.iterrows():
            first_5_columns = row[1:6]
            sell_price = row['sell_price']
            
            price_diff = sell_price - base_price
            price_discount = round(((base_price - sell_price)/base_price * 100), 2)
            
            demand_change = row['Summary'][year]['avg_sold_wk'] - base_demand
            
            if base_demand == 0 and demand_change != 0:
                demand_percent = 100
            elif base_demand == 0 and demand_change == 0:
                demand_percent = 0
            else:
                demand_percent = round(demand_change/base_demand * 100, 2)
            
            new_row = pd.concat([first_5_columns, pd.Series({'base_price': base_price,
                                                             'sell_price': sell_price, 
                                                             'price_diff': price_diff, 
                                                             'price_discount': price_discount,
                                                             'base_demand': base_demand,
                                                             'demand_change': demand_change,
                                                             'demand_percent': demand_percent})])
            
            result_df = result_df._append(new_row, ignore_index=True)
    
    # print(len(result_df))
    
    temp_df = result_df[(result_df['demand_change'] == 0) & (result_df['price_diff'] == 0)].copy()
    temp_df['id'] = temp_df['item_id'] + '_' + temp_df['store_id']

    # print(len(temp_df['id'].unique()))
    # print(len(temp_df))
    
    temp_df = temp_df[~temp_df.duplicated(subset='id', keep='first')]
    
    indices_to_remove = temp_df.index
    result_df = result_df.drop(indices_to_remove)
    result_df.reset_index(drop=True, inplace=True)
    
    # print(len(result_df))
    
    return result_df

if __name__ == '__main__':
    salesDF = pd.read_csv("sales.csv")
    priceDF = pd.read_csv("price.csv")
    
    salesDF['Summary'] = salesDF['Summary'].apply(ast.literal_eval)
    priceDF['Base Price'] = priceDF['Base Price'].apply(ast.literal_eval)
    priceDF['Price Count'] = priceDF['Price Count'].apply(ast.literal_eval)
    
    salesDF['start_date'] = pd.to_datetime(salesDF['start_date'])
    salesDF['end_date'] = pd.to_datetime(salesDF['end_date'])
    
    data_2011 = get_dataset(priceDF, salesDF, 2011)
    data_2011.to_csv("modelData/data_2011.csv", index=False)
    
    data_2012 = get_dataset(priceDF, salesDF, 2012)
    data_2012.to_csv("modelData/data_2012.csv", index=False)
    
    data_2013 = get_dataset(priceDF, salesDF, 2013)
    data_2013.to_csv("modelData/data_2013.csv", index=False)
    
    data_2014 = get_dataset(priceDF, salesDF, 2014)
    data_2014.to_csv("modelData/data_2014.csv", index=False)
    
    data_2015 = get_dataset(priceDF, salesDF, 2015)
    data_2015.to_csv("modelData/data_2015.csv", index=False)
    
    data_2016 = get_dataset(priceDF, salesDF, 2016)
    data_2016.to_csv("modelData/data_2016.csv", index=False)