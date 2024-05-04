import json
import pandas as pd
import boto3
import ast

from getData import getYearList

__author__ = "Hamid Abrar Mahir (32226136), Setyawan Prayogo (32213816), Yuan She (32678304), Regina Lim (32023863)"

def levelSelection(priceDF, data, year: int, store_id: str, item_id: str, event: bool, snap: bool):
    
    # Previous year
    year -= 1
    
    # Check year is valid first
    if year not in getYearList(priceDF, store_id, item_id, event, snap):
        # Improvement: put minimum year that is valid
        return "Year Invalid"
    
    productInfo = getProductInfo(store_id, item_id)
    
    for key, value in reversed(productInfo.items()):
        if key in ["item_id", "dept_id", "cat_id"]:
            filter_condition = "store_id == " + f"'{productInfo['store_id']}'" + " and " + f"{key} == '{value}'"
        else: 
            filter_condition =  f"{key} == '{value}'"

        filteredDF = data.query(filter_condition)
        # print(key)
        # print(filteredDF['sell_price'].nunique())
        # print(len(filteredDF))
        
        if len(filteredDF) >= 500:
            level = key
            break
        
    return level

def getProductInfo(store_id: str, item_id: str):
    state_id = store_id.split('_')[0]
    dept_id = '_'.join(item_id.split('_')[:2])
    cat_id = item_id.split('_')[0]
    
    return {'state_id': state_id, 'store_id': store_id, 'cat_id': cat_id, 'dept_id': dept_id, 'item_id': item_id}


# if __name__ == '__main__':
#     # Get access key
#     credentials = json.load(open('credentials.json'))

#     access_key = credentials.get('aws_access_key_id')
#     secret_key = credentials.get('aws_secret_access_key')

#     s3 = boto3.resource(
#         service_name='s3',
#         region_name='ap-southeast-2',
#         aws_access_key_id=access_key,
#         aws_secret_access_key=secret_key
#     )
    
#     priceDF = pd.read_csv(s3.Bucket(name='fit3164-bucket').Object('price.csv').get()['Body'])

#     priceDF['basePrice_withoutBoth'] = priceDF['basePrice_withoutBoth'].apply(ast.literal_eval)
#     priceDF['basePrice_withBoth'] = priceDF['basePrice_withBoth'].apply(ast.literal_eval)
#     priceDF['basePrice_onlyEvent'] = priceDF['basePrice_onlyEvent'].apply(ast.literal_eval)
#     priceDF['basePrice_onlySNAP'] = priceDF['basePrice_onlySNAP'].apply(ast.literal_eval)
#     priceDF['Price Count'] = priceDF['Price Count'].apply(ast.literal_eval)
    
#     year = 2015
#     store_id = "CA_1"
#     item_id = "FOODS_1_010"
    
#     levelSelection(priceDF, year, store_id, item_id)