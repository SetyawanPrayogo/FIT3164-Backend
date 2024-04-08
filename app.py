# import dataTransformation
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

salesDF = pd.read_csv("sales.csv")
priceDF = pd.read_csv("price.csv")

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


@app.route('/getPriceElasticity', methods=['GET'])
def main():   
    year = 2015
    store_id = "CA_1"
    item_id = "FOODS_1_073"
    
    model = priceElasticityModel.createModel(salesDF, priceDF, year, store_id, item_id)
    return model

# for testing with frontend, comment the next2 lines
if __name__ == '__main__':
    app.run(debug=True)


# For testing with frontend
if __name__ == '__main__':
    main()