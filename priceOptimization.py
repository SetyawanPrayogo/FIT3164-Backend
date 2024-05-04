import pandas as pd
import ast
import numpy as np
from priceElasticityModel import createModel, getBase, predictDemand

__author__ = "Hamid Abrar Mahir (32226136), Setyawan Prayogo (32213816), Yuan She (32678304), Regina Lim (32023863)"

def priceOptimization(base_price, cost_price, target_date, stock_on_hand, poly, model, current_date, discount_range):
    
    days_until_target = (target_date - current_date).days
    stock_to_sales_ratio = stock_on_hand / base_demand  

    # Iterate through a range of discount values to find the optimal price
    for discount in discount_range:
        # the price after applying the discount
        price_after_discount = base_price * (1 - discount / 100)
        if price_after_discount < cost_price:
            continue  
        
        # Predict the demand using the polynomial regression model
        demand_predicted = predictDemand(poly, model, discount)

        # the revenue for the current price and demand
        revenue = price_after_discount * demand_predicted

        # Adjust revenue based on days until target and stock levels
        if days_until_target < 10:  
            revenue *= 0.95  

        if stock_to_sales_ratio > 1.5:  
            revenue *= 1.1  

        # Update the optimal price and demand if the revenue is higher
        if revenue > max_revenue:
            optimal_price = price_after_discount
            optimal_demand = demand_predicted
            max_revenue = revenue

    return optimal_price, optimal_demand

if __name__ == '__main__':
    salesDF = pd.read_csv("sales.csv")
    priceDF = pd.read_csv("price.csv")
    
    salesDF['Summary'] = salesDF['Summary'].apply(ast.literal_eval)
    priceDF['Base Price'] = priceDF['Base Price'].apply(ast.literal_eval)
    priceDF['Price Count'] = priceDF['Price Count'].apply(ast.literal_eval)
    
    salesDF['start_date'] = pd.to_datetime(salesDF['start_date'])
    salesDF['end_date'] = pd.to_datetime(salesDF['end_date'])
    
    year = 2014
    store_id = "TX_1"
    item_id = "HOUSEHOLD_2_466"
    
    base_price, base_demand = getBase(salesDF, priceDF, year, store_id, item_id)
    poly, model, rmse = createModel(salesDF, priceDF, year, store_id, item_id, deg=3)
    
    optimal_price, optimal_demand = priceOptimization(base_price, base_demand, poly, model, year, discount_range=(0, 50))
    print(f"Optimal price for {item_id} at {store_id} in {year} is: ${optimal_price:.2f}")
    print(f"Optimal demand for {item_id} at {store_id} in {year} is: {optimal_demand:.2f}")
