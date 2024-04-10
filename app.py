# import dataTransformation
import boto3
import json
import pandas as pd
import ast
import priceElasticityModel
from flask import Flask, jsonify
from flask_cors import CORS
from flask import request

app = Flask(__name__)
CORS(app)

stores = {
    'st1Cal': 'CA_1',
    'st2Cal': 'CA_2',
    'st3Cal': 'CA_3',
    'st4Cal': 'CA_4',
    'st1Tex': 'TX_1',
    'st2Tex': 'TX_2',
    'st3Tex': 'TX_3',
    'st1Win': 'WI_1',
    'st2Win': 'WI_2',
    'st3Win': 'WI_3',
}

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

salesDF= pd.read_csv(s3.Bucket(name='fit3164-bucket').Object('sales.csv').get()['Body'])
priceDF = pd.read_csv(s3.Bucket(name='fit3164-bucket').Object('price.csv').get()['Body'])

salesDF['Summary'] = salesDF['Summary'].apply(ast.literal_eval)
priceDF['Base Price'] = priceDF['Base Price'].apply(ast.literal_eval)
priceDF['Price Count'] = priceDF['Price Count'].apply(ast.literal_eval)

salesDF['start_date'] = pd.to_datetime(salesDF['start_date'])
salesDF['end_date'] = pd.to_datetime(salesDF['end_date'])

# get all items from a store
# run this in the browser: http://127.0.0.1:5000/get-items/st1Cal . This will return all the items in store 1 in California. Make sure the application is running using flask run
@app.route('/get-items/<storeId>', methods=['GET'])
def getItems(storeId):
    storeId = stores.get(storeId, "Could not find store")
    print(storeId)

    # only show the unique items in the store
    items = salesDF[salesDF['store_id'] == storeId]['item_id'].unique()
    return jsonify(items.tolist())  # Convert items to a list


# Input argument, storeId and itemId.
# Outputs all the years that item was sold in that store
# run this in the browser: http://127.0.0.1:5000/get-year?storeId=st1Cal&itemId=FOODS_1_001
@app.route('/get-year', methods=['GET'])
def get_year():
    store_id = request.args.get('storeId')
    item_id = request.args.get('itemId')
    storeId = stores.get(store_id, "Could not find store")
    productPriceInfo = priceDF[(priceDF['store_id'] == storeId) & (priceDF['item_id'] == item_id)].reset_index()
    data = productPriceInfo['Price Count'].iloc[0]
    yearList =  [year for year, count in data.items() if count != 0]
    yearList.pop(0)
    return jsonify(yearList)


# Comment this code if you want to work on the backend only. This code will only work with the frontend.
# http://127.0.0.1:5000/get-price-elasticity?storeId=st1Cal&itemId=FOODS_1_001&yearId=2015
@app.route('/get-price-elasticity', methods=['GET'])
def main():   
    store_id = request.args.get('storeId')
    item_id = request.args.get('itemId')
    year_id = request.args.get('yearId')

    storeId = stores.get(store_id, "Could not find store")

    year = int(year_id)
    store_id = storeId
    item_id = item_id

    print("#############################################")
    print(year, store_id, item_id)
    
    # To show in the Front-end
    base_price, base_demand = priceElasticityModel.getBase(salesDF, priceDF, year, store_id, item_id)
    
    # Not gonna show on UI
    poly, model, rmse = priceElasticityModel.createModel(salesDF, priceDF, year, store_id, item_id, deg = 3)

    # 60 is the user input (discount)
    # y_predict is the change on demand (percent)
    y_predict = priceElasticityModel.predictDemand(poly, model, 60)
    
    print("Printing the results", base_price, base_demand, rmse, y_predict)
    return {'base_price': base_price, 'base_demand': base_demand, 'rmse': rmse, 'y_predict': y_predict}



# Uncomment this code when you want to work on the backend only
"""
def main():   
    print("#############################################")

    year = 2015
    store_id = 'CA_1'
    item_id = 'FOODS_1_001'
    
    base_price, base_demand = priceElasticityModel.getBase(salesDF, priceDF, year, store_id, item_id)
    
    poly, model, rmse = priceElasticityModel.createModel(salesDF, priceDF, year, store_id, item_id, deg = 3)

    y_predict = priceElasticityModel.predictDemand(poly, model, 60)
    
    print("Printing the results", base_price, base_demand, rmse, y_predict)
    return {'base_price': base_price, 'base_demand': base_demand, 'rmse': rmse, 'y_predict': y_predict}
"""


# for testing with frontend
if __name__ == '__main__':
    app.run(debug=True)


# For testing with backend
# if __name__ == '__main__':
#     main()