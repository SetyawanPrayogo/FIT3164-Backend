import pandas as pd
import ast
from selectLevel import levelSelection

__author__ = "Hamid Abrar Mahir (32226136), Setyawan Prayogo (32213816), Yuan She (32678304), Regina Lim (32023863)"

def createModel(salesDF, priceDF, year: int, store_id: str, item_id: str):
    productPriceInfo = priceDF[(priceDF['store_id'] == store_id) & (priceDF['item_id'] == item_id)]
    print(productPriceInfo)
    
    if productPriceInfo['Price Count'].iloc[0][year-1] == 0: 
        # Improvement: put minimum year that is valid
        return "Year Invalid"
    
    level = levelSelection(priceDF, year, store_id, item_id)
    print(level)
    
    # base_price = 
    # base_demand = 

    return year


if __name__ == '__main__':
    salesDF = pd.read_csv("sales.csv")
    priceDF = pd.read_csv("price.csv")
    
    salesDF['Summary'] = salesDF['Summary'].apply(ast.literal_eval)
    priceDF['Base Price'] = priceDF['Base Price'].apply(ast.literal_eval)
    priceDF['Price Count'] = priceDF['Price Count'].apply(ast.literal_eval)
    
    year = 2012
    store_id = "CA_1"
    item_id = "FOODS_1_001"
    
    model = createModel(salesDF, priceDF, year, store_id, item_id)