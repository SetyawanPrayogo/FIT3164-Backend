import pandas as pd
import ast
from selectLevel import levelSelection

__author__ = "Hamid Abrar Mahir (32226136), Setyawan Prayogo (32213816), Yuan She (32678304), Regina Lim (32023863)"

def createModel(salesDF, priceDF, year: int, store_id: str, item_id: str):
    productPriceInfo = priceDF[(priceDF['store_id'] == store_id) & (priceDF['item_id'] == item_id)].reset_index()
    productSalesInfo = salesDF[(salesDF['item_id'] == item_id) & (salesDF['store_id'] == store_id)].reset_index()
    # print(productPriceInfo)
    print(productSalesInfo)
    
    if productPriceInfo['Price Count'].iloc[0][year-1] == 0: 
        # Improvement: put minimum year that is valid
        return "Year Invalid"

    level = levelSelection(priceDF, year, store_id, item_id)
    print(level)
    
    base_price = productPriceInfo['Base Price'][0][year-1]
    print("base price: ")
    print(base_price)

    base_price_row = productSalesInfo[(productSalesInfo['sell_price'] == base_price) & (productSalesInfo['start_date'].dt.year == year - 1)].reset_index()
    print(base_price_row)

    if len(base_price_row) == 1:
        base_demand = base_price_row['Summary'][0][year-1]['avg_sold_wk']
        print("base demand: ")
        print(base_demand)
    else:
        base_demand = base_price_row['Summary'][0][year-1]['avg_sold_wk']
        for i in range(1,len(base_price_row)):
            current_base_demand = base_price_row['Summary'][i][year-1]['avg_sold_wk']
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
    
    salesDF['start_date'] = pd.to_datetime(salesDF['start_date'])
    salesDF['end_date'] = pd.to_datetime(salesDF['end_date'])
    
    year = 2015
    store_id = "CA_1"
    item_id = "FOODS_1_073"
    
    model = createModel(salesDF, priceDF, year, store_id, item_id)