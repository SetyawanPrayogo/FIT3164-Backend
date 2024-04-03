import pandas as pd

__author__ = "Hamid Abrar Mahir (32226136), Setyawan Prayogo (32213816), Yuan She (32678304), Regina Lim (32023863)"

def levelSelection(priceDF, year: int, store_id: str, item_id: str):
    # Previous year
    year -= 1
    
    productInfo = getProductInfo(store_id, item_id)
    
    for key, value in reversed(productInfo.items()):
        if key in ["item_id", "dept_id", "cat_id"]:
            filter_condition = "store_id == " + f"'{productInfo['store_id']}'" + " and " + f"{key} == '{value}'"
        else: 
            filter_condition =  f"{key} == '{value}'"

        filteredDF = priceDF.query(filter_condition)
                
        price_count = filteredDF['Price Count'].apply(lambda x: x[year]).apply(lambda x: x - 1 if x != 0 else x).sum()
        
        if price_count >= 100:
            level = key
            break
    
    return level

def getProductInfo(store_id: str, item_id: str):
    state_id = store_id.split('_')[0]
    dept_id = '_'.join(item_id.split('_')[:2])
    cat_id = item_id.split('_')[0]
    
    return {'state_id': state_id, 'store_id': store_id, 'cat_id': cat_id, 'dept_id': dept_id, 'item_id': item_id}


# if __name__ == '__main__':
#     priceDF = pd.read_csv("price.csv")
    
#     priceDF['Base Price'] = priceDF['Base Price'].apply(ast.literal_eval)
#     priceDF['Price Count'] = priceDF['Price Count'].apply(ast.literal_eval)
    
#     PRINT item_id
#     year = 2015
#     store_id = "CA_1"
#     item_id = "FOODS_1_010"

#     dept_id
#     year = 2012
#     store_id = "CA_1"
#     item_id = "FOODS_1_001"
    
#     a = levelSelection(priceDF, year, store_id, item_id)