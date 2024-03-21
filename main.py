# import dataTransformation
import pandas as pd
import ast
import priceElasticityModel

def main():
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
    
    model = priceElasticityModel.createModel(salesDF, priceDF, year, store_id, item_id)
    # print(model)
    return model
    

if __name__ == '__main__':
    main()
    