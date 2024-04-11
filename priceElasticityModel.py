import boto3
import json
import pandas as pd
import ast

import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from selectLevel import levelSelection, getProductInfo

__author__ = "Hamid Abrar Mahir (32226136), Setyawan Prayogo (32213816), Yuan She (32678304), Regina Lim (32023863)"

def createModel(salesDF, priceDF, year: int, store_id: str, item_id: str, deg: int):
    productPriceInfo = priceDF[(priceDF['store_id'] == store_id) & (priceDF['item_id'] == item_id)].reset_index()
    productSalesInfo = salesDF[(salesDF['item_id'] == item_id) & (salesDF['store_id'] == store_id)].reset_index()
    # print(productPriceInfo)
    # print(productSalesInfo)
    
    if productPriceInfo['Price Count'].iloc[0][year-1] == 0: 
        # Improvement: put minimum year that is valid
        return "Year Invalid"

    # Choose the level of the model
    level = levelSelection(priceDF, year, store_id, item_id)
    print(level)
    
    # get data for model
    dataModel = getData(year-1)
    
    # get the productInfo (state_id, store_id, cat_id, dept_id, item_id)
    productInfo = getProductInfo(store_id, item_id)
    
    # filter the data based on the level
    dataModel = filterData(dataModel, productInfo, level)
    # sort dataModel by price_discount (x axis)
    dataModel = dataModel.sort_values(by='price_discount')
    # print(dataModel.shape)
    
    x = dataModel['price_discount']
    y = dataModel['demand_percent']

    # Generate values 0 to 100 by 0.01
    x_values = np.arange(0, 100.01, 0.01)
    
    # Degree ?? - To think
    poly = PolynomialFeatures(degree=deg, include_bias=False)
    X = poly.fit_transform(x.to_numpy().reshape(-1, 1))
    poly_reg_model = LinearRegression()
    poly_reg_model.fit(X, y)
    
    # Prediction
    x_values_poly = poly.fit_transform(x_values.reshape(-1, 1))
    y_predicted = poly_reg_model.predict(x_values_poly)
    
    # Calcualte RMSE
    actual_y_predicted = poly_reg_model.predict(X)
    rmse = np.sqrt(mean_squared_error(y, actual_y_predicted))
    print("RMSE:", rmse)

    plt.figure(figsize=(10, 6))
    plt.scatter(x, y, label='Actual Data')
    plt.plot(x_values, y_predicted, c="red", label='Polynomial Regression')
    plt.title(f"Price Elasticity Analysis for {item_id} in Store {store_id} at {level} Level")
    plt.xlabel('Price Discount (%)')
    plt.ylabel('Change in Demand (%)')
    plt.legend()
    #* Uncomment the next line if you want to work with backend
    #* Comment the next line if you want to work with frontend
    # plt.show()
            
    return poly, poly_reg_model, rmse

def predictDemand(poly, model, base_demand, discount):
    x_predict_poly = poly.fit_transform(np.array(discount).reshape(-1, 1))
    y_predict = model.predict(x_predict_poly)
    
    y_predict[0] = round(y_predict[0], 2)
    
    if y_predict[0] == 0:
        impact = f"No Impact ({y_predict[0]}% change)"
    elif y_predict[0] > 0:
        impact = f"Increased by {y_predict[0]}%"
    elif y_predict[0] < 0:
        impact = f"Decreased by {abs(y_predict[0])}%"
        
    demand = f"{round(base_demand + base_demand * y_predict[0]/100, 2)} Items sold per week"
    
    return impact, demand

def getData(year):
    filePath = f"modelData/data_{year}.csv"
    
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
        
    data = pd.read_csv(s3.Bucket(name='fit3164-bucket').Object(filePath).get()['Body'])
    
    return data

def filterData(data, productInfo, level: str):
    if level in ["item_id", "dept_id", "cat_id"]:
        store_id = productInfo.get("store_id")
        filter_product = productInfo.get(level)
        filter_value = "store_id == " + f"'{store_id}'" + " and " + f"{level} == '{filter_product}'"
        # print(filter_value)
    else: 
        filter_product = productInfo.get(level)
        filter_value = f"{level} == '{filter_product}'"
    
    data = data.query(filter_value).reset_index(drop=True)
    
    return data

def getBase(salesDF, priceDF, year: int, store_id: str, item_id: str):
    productPriceInfo = priceDF[(priceDF['store_id'] == store_id) & (priceDF['item_id'] == item_id)].reset_index()
    productSalesInfo = salesDF[(salesDF['item_id'] == item_id) & (salesDF['store_id'] == store_id)].reset_index()
    
    if productPriceInfo['Price Count'].iloc[0][year-1] == 0: 
        # Improvement: put minimum year that is valid
        return "Year Invalid"

    base_price = productPriceInfo['Base Price'][0][year-1]
    # print("base price: ")
    # print(base_price)

    base_price_row = productSalesInfo[(productSalesInfo['sell_price'] == base_price) & (productSalesInfo['start_date'].dt.year <= year - 1) & 
                                  (productSalesInfo['end_date'].dt.year >= year - 1)].reset_index()
    # print(base_price_row)

    if len(base_price_row) == 1:
        base_demand = base_price_row['Summary'][0][year-1]['avg_sold_wk']
    else:
        base_demand = base_price_row['Summary'][0][year-1]['avg_sold_wk']
        for i in range(1,len(base_price_row)):
            current_base_demand = base_price_row['Summary'][i][year-1]['avg_sold_wk']
            if current_base_demand < base_demand:
                base_demand = current_base_demand
                
    # print("base demand: ")
    # print(base_demand)
    
    return base_price, base_demand
    
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

    salesDF= pd.read_csv(s3.Bucket(name='fit3164-bucket').Object('sales.csv').get()['Body'])
    priceDF = pd.read_csv(s3.Bucket(name='fit3164-bucket').Object('price.csv').get()['Body'])
    
    salesDF['Summary'] = salesDF['Summary'].apply(ast.literal_eval)
    priceDF['Base Price'] = priceDF['Base Price'].apply(ast.literal_eval)
    priceDF['Price Count'] = priceDF['Price Count'].apply(ast.literal_eval)
    
    salesDF['start_date'] = pd.to_datetime(salesDF['start_date'])
    salesDF['end_date'] = pd.to_datetime(salesDF['end_date'])
    
    # dept_id
    year = 2014
    store_id = "TX_1"
    item_id = "HOUSEHOLD_2_466"
    
    base_price, base_demand = getBase(salesDF, priceDF, year, store_id, item_id)
    base_price
    base_demand
    
    poly, model, rmse = createModel(salesDF, priceDF, year, store_id, item_id, deg = 3)
    plt.show()

    impact, demand = predictDemand(poly, model, base_demand, 60)
    impact
    demand