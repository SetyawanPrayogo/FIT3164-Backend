import boto3
import json
import pandas as pd
import ast

from priceElasticityModel import createModel, predictDemand
from getData import getBase, getYearList

import unittest
from priceOptimization import getOptimizedPrice, calculateRevenue, getDummyData, getCostStock

__author__ = "Hamid Abrar Mahir (32226136), Setyawan Prayogo (32213816), Yuan She (32678304), Regina Lim (32023863)"

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

demandDF = pd.read_csv(s3.Bucket(name='fit3164-bucket').Object('demand.csv').get()['Body'])

demandDF['withoutBoth'] = demandDF['withoutBoth'].apply(ast.literal_eval)
demandDF['withBoth'] = demandDF['withBoth'].apply(ast.literal_eval)
demandDF['onlyEvent'] = demandDF['onlyEvent'].apply(ast.literal_eval)
demandDF['onlySNAP'] = demandDF['onlySNAP'].apply(ast.literal_eval)

demandDF['start_date'] = pd.to_datetime(demandDF['start_date'])
demandDF['end_date'] = pd.to_datetime(demandDF['end_date'])


class TestPriceOptimization(unittest.TestCase):
    def test_getOptimizedPrice(self):
        price_1 = getOptimizedPrice()  # revenueList, stockCost
        self.assertEqual(price_1)  # discount, optimizedPrice, totalSold, totalRevenue, profitLoss, totalDay

    def test_calculateRevenue(self):
        revenue_1 = calculateRevenue(demandDF, priceDF, 2011, 'CA_1', 'FOODS_1_001', event=False, snap=True, eventCount=0, snapCount=3) 
        self.assertEqual(revenue_1)  # costPrice, stockOnHand, revenueList, stockCost

    def test_getDummyData_and_getCostStock(self):
        data_1 = getDummyData(2011)  
        self.assertEqual(len(data_1), 16763)  

        data_2 = getDummyData(2012)  
        self.assertEqual(len(data_2), 21895)

        data_3 = getDummyData(2013)  
        self.assertEqual(len(data_3), 25940)

        data_4 = getDummyData(2014)  
        self.assertEqual(len(data_4), 29585)

        data_5 = getDummyData(2015)  
        self.assertEqual(len(data_5), 30475)

        data_6 = getDummyData(2016)  
        self.assertEqual(len(data_6), 30491)


        stock_1 = getCostStock(data_1, 'CA_1', 'FOODS_1_001')  
        self.assertEqual(stock_1, 1, 95)  

        stock_2 = getCostStock(data_2, 'CA_1', 'FOODS_1_001')  
        self.assertEqual(stock_2, 1.12, 97)

        stock_3 = getCostStock(data_3, 'CA_1', 'FOODS_1_001')  
        self.assertEqual(stock_3, 1.12, 92)

        stock_4 = getCostStock(data_4, 'CA_1', 'FOODS_1_001')  
        self.assertEqual(stock_4, 1.12, 93)

        stock_5 = getCostStock(data_5, 'CA_1', 'FOODS_1_001')  
        self.assertEqual(stock_5, 1.12, 93)

        stock_6 = getCostStock(data_6, 'CA_1', 'FOODS_1_001')  
        self.assertEqual(stock_6, 1.12, 94)


if __name__ == '__main__': 
    unittest.main()

