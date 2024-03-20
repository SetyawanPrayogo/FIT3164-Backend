import pandas as pd
import ast
from selectLevel import levelSelection

__author__ = "Hamid Abrar Mahir (32226136), Setyawan Prayogo (32213816), Yuan She (32678304), Regina Lim (32023863)"

def createModel(salesDF, priceDF, year: int, store_id: str, item_id: str):
    productPriceInfo = priceDF[(priceDF['store_id'] == store_id) & (priceDF['item_id'] == item_id)]
    productSalesInfo = salesDF[(salesDF['item_id'] == item_id) & (salesDF['store_id'] == store_id)]
    # print(productPriceInfo)
    print(productSalesInfo)
    
    if productPriceInfo['Price Count'].iloc[0][year-1] == 0: 
        # Improvement: put minimum year that is valid
        return "Year Invalid"

    level = levelSelection(priceDF, year, store_id, item_id)
    print(level)
    
    base_price = productPriceInfo['Base Price'].iloc[0][year-1]
    print("base price: ")
    print(base_price)

    # Extract the year from the start_date column and convert it to an integer
    productSalesInfo['start_year'] = productSalesInfo['start_date'].str[:4].astype(int)

    # Filter the DataFrame based on the condition
    previous_year_data = productSalesInfo[productSalesInfo['start_year'] == year - 1]
    print("previous_year_data:")
    print(previous_year_data)

    # Find rows with maximum sell_price
    max_sell_price_rows = previous_year_data[previous_year_data['sell_price'] == base_price].reset_index()
    print("rows with max sell_price:")
    print(max_sell_price_rows)

    if len(max_sell_price_rows) == 1:
        base_demand = max_sell_price_rows['Summary'][0][year-1]['avg_sold_wk']
        print("base demand: ")
        print(base_demand)
    else:
        base_demand = max_sell_price_rows['Summary'][0][year-1]['avg_sold_wk']
        for i in range(1,len(max_sell_price_rows)):
            current_base_demand = max_sell_price_rows['Summary'][i][year-1]['avg_sold_wk']
            if current_base_demand < base_demand:
                base_demand = current_base_demand
        print(base_demand)
            
    return year


if __name__ == '__main__':
    salesDF = pd.read_csv("sales.csv")
    priceDF = pd.read_csv("price.csv")
    
    salesDF['Summary'] = salesDF['Summary'].apply(ast.literal_eval)
    priceDF['Base Price'] = priceDF['Base Price'].apply(ast.literal_eval)
    priceDF['Price Count'] = priceDF['Price Count'].apply(ast.literal_eval)
    
    year = 2013
    store_id = "CA_1"
    item_id = "FOODS_1_010"
    
    model = createModel(salesDF, priceDF, year, store_id, item_id)