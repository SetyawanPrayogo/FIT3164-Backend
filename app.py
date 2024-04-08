# import dataTransformation
import pandas as pd
import ast
import priceElasticityModel
from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# get all items
@app.route('/getItems', methods=['GET'])
def getItems():
    salesDF = pd.read_csv("sales.csv")
    items = salesDF['item_id'].unique()
    return jsonify(items.tolist())

@app.route('/getPriceElasticity', methods=['GET'])
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
    return model

# for testing with frontend, comment the next2 lines
if __name__ == '__main__':
    app.run(debug=True)


# For testing with frontend
if __name__ == '__main__':
    main()