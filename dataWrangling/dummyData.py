import pandas as pd
import numpy as np

__author__ = "Hamid Abrar Mahir (32226136), Setyawan Prayogo (32213816), Yuan She (32678304), Regina Lim (32023863)"

def generateDummyData(data, dataModel):
    data['id'] = data['store_id'] + '_' + data['item_id']
    
    # Create a new dataset
    newDF = data.copy(deep=True)
    newDF = newDF.drop_duplicates(subset='id')
    newDF = newDF[['state_id', 'store_id', 'cat_id', 'dept_id', 'item_id']]
    
    # Get minimum base price
    minBasePrice = data.groupby(['store_id', 'item_id'])['basePrice'].min().reset_index()
    newDF = newDF.merge(minBasePrice, on=['store_id', 'item_id'], how='left')
    
    # Cost Price = basePrice / 2
    newDF['costPrice'] = round(newDF['basePrice']/2, 2)
    
    # Get stocks
    maxBaseDemand = dataModel.groupby(['store_id', 'item_id'])['baseDemand'].max().reset_index()
    # Change to 1 if baseDemand = 0
    maxBaseDemand.loc[maxBaseDemand['baseDemand'] == 0, 'baseDemand'] = 1
    
    # Calculate boundary for stockOnHand
    maxBaseDemand['max_stock'] = maxBaseDemand['baseDemand'] * 1.5
    maxBaseDemand.loc[maxBaseDemand['max_stock'] < 100, 'max_stock'] = 100
    
    newDF = newDF.merge(maxBaseDemand, on=['store_id', 'item_id'], how='left')
    # Round up for minimum randomize
    newDF['baseDemand'] = np.ceil(newDF['baseDemand'])
    
    np.random.seed(9999)
    newDF['stockOnHand'] = np.random.randint(np.ceil(newDF['baseDemand']*1.1), newDF['max_stock'] + 1)
    
    newDF = newDF[['state_id', 'store_id', 'cat_id', 'dept_id', 'item_id', 'costPrice', 'stockOnHand']]
    
    return newDF

if __name__ == '__main__':
    for year in range(2011, 2017):
        data = pd.read_csv(f"trueFalse/data_{year}.csv")
        dataModel = pd.read_csv(f"../modelData/data_{year}.csv")
        
        newData = generateDummyData(data, dataModel)
        newData.to_csv(f"../dummyData/data_{year}.csv", index=False)