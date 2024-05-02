import boto3
import json
import pandas as pd
import ast

import numpy as np
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split

from selectLevel import levelSelection, getProductInfo
from getData import getBase, getYearList
from priceElasticityModel import createModel

def testModel(priceDF, data, year: int):
    results = []
    
    for store_id in data['store_id'].unique():
        storeData = data[data['store_id'] == store_id]
        uniqueId = len(data['item_id'].unique())
        randomIndex = np.random.randint(uniqueId, size=4)
        item_id = data['item_id'][randomIndex]
    for data_id in data['id'].unique():
        store_id = data_id[:4]
        item_id = data_id[5:]
        for event in [True, False]:
            for snap in [True, False]:
                # print(year, store_id, item_id, event, snap)
                result = createModel(priceDF, year+1, store_id, item_id, event, snap)
                # print(result)
                if result == 'Year Invalid':
                    break
                else:
                    results.append({'store_id': store_id, 'item_id': item_id, 'event': event, 'snap': snap, 'RMSE': result[2], 'score': result[3]})
    
    results = pd.DataFrame(results)
    
    return results
        
    

if __name__ == '__main__':
     # Get access key
    credentials = json.load(open('credentials.json'))

    access_key = credentials.get('aws_access_key_id')
    secret_key = credentials.get('aws_secret_access_key')

    s3 = boto3.resource(
        service_name='s3',
        region_name='ap-southeast-2',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    
    priceDF = pd.read_csv(s3.Bucket(name='fit3164-bucket').Object('price.csv').get()['Body'])
    
    priceDF['basePrice_withoutBoth'] = priceDF['basePrice_withoutBoth'].apply(ast.literal_eval)
    priceDF['basePrice_withBoth'] = priceDF['basePrice_withBoth'].apply(ast.literal_eval)
    priceDF['basePrice_onlyEvent'] = priceDF['basePrice_onlyEvent'].apply(ast.literal_eval)
    priceDF['basePrice_onlySNAP'] = priceDF['basePrice_onlySNAP'].apply(ast.literal_eval)
    priceDF['Price Count'] = priceDF['Price Count'].apply(ast.literal_eval)
    
    filePath = f"modelData/data_2011.csv"
    data = pd.read_csv(s3.Bucket(name='fit3164-bucket').Object(filePath).get()['Body'])
    
    dataTest = testModel(priceDF, data, 2011)