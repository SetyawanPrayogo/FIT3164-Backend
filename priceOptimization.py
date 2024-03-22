import pandas as pd
import ast
from priceElasticityModel import createModel

__author__ = "Hamid Abrar Mahir (32226136), Setyawan Prayogo (32213816), Yuan She (32678304), Regina Lim (32023863)"

def priceOptimization(base_price, base_demand, previous_year_data, year):
    mean_sales = base_demand
    std_sales = previous_year_data['Summary'].apply(lambda x: x[year - 1]['avg_sold_wk']).std()
    scaling_factor = 1 + (0.1 * (base_demand - mean_sales) / std_sales)

    adj_price = base_price * scaling_factor

    return adj_price


if __name__ == '__main__':
    salesDF = pd.read_csv("sales.csv")
    priceDF = pd.read_csv("price.csv")

    salesDF['Summary'] = salesDF['Summary'].apply(ast.literal_eval)
    priceDF['Base Price'] = priceDF['Base Price'].apply(ast.literal_eval)
    priceDF['Price Count'] = priceDF['Price Count'].apply(ast.literal_eval)

    year = 2013
    store_id = "CA_1"
    item_id = "FOODS_1_010"

    base_price, base_demand = createModel(salesDF, priceDF, year, store_id, item_id)
    if base_price != "Year Invalid":
        previous_year_data = salesDF[(salesDF['item_id'] == item_id) & (salesDF['store_id'] == store_id) & (
                    salesDF['start_date'].str[:4].astype(int) == year - 1)]
        optimal_price = priceOptimization(base_price, base_demand, previous_year_data, year)
        print(f"Optimal price for {item_id} at {store_id} in {year} is: ${optimal_price:.2f}")
    else:
        print("Year is invalid.")
