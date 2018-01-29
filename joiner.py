import json
import os
import pandas as pd
import csv
from pprint import pprint
import argparse

def loadJSON(filename):
    with open(filename) as data_file:    
        data = json.load(data_file)
    # pprint(data)
    return data

def json2csv(filepath1,filepath2):
    customers_json = open(filepath1).read()
    customers_parsed = json.loads(customers_json)
    customers_csv = open('customers.csv','w')
    writer = csv.writer(customers_csv)
    writer.writerow(['cid','name'])   
    for item in customers_parsed:
        writer.writerow([item['cid'],item['name']])
    orders_json = open(filepath2).read()
    orders_parsed = json.loads(orders_json)
    orders_csv = open('orders.csv','w')
    writer = csv.writer(orders_csv)
    writer.writerow(['oid','customer_id','name'])   
    for item in orders_parsed:
        writer.writerow([ item['oid'] , item['customer_id'] , item['price'] ]) 



def innerJoin(key, left_table, right_table):
    customers_df = pd.read_csv('customers.csv')
    orders_df = pd.read_csv('orders.csv')
    if key == 'cid':
        orders_df.columns = ['oid','cid','name']
        print ("For key : ", key)
        innerJoin      = pd.merge(customers_df,orders_df, on=key)
        rowsSkippedLeft = len(left_table) - len(innerJoin.drop_duplicates('cid'))
        rowsSkippedRight = len(right_table) - len(innerJoin.drop_duplicates('cid'))
        output = {'Result Count': len(innerJoin), 'Rows Skipped on Left': rowsSkippedLeft  ,'Rows Skipped on Right': rowsSkippedRight, 'Result': innerJoin.to_json(orient='table')}
        pprint(output) 
    elif key == 'customer_id':
        customers_df.columns = ['customer_id','name']
        print ('For key : ', key)
        innerJoin      = pd.merge(customers_df,orders_df, on=key)
        rowsSkippedLeft = len(left_table) - len(innerJoin.drop_duplicates('customer_id'))
        rowsSkippedRight = len(right_table) - len(innerJoin.drop_duplicates('customer_id'))
        output = {'Result Count': len(innerJoin), 'Rows Skipped on Left': rowsSkippedLeft  ,'Rows Skipped on Right': rowsSkippedRight, 'Result': innerJoin.to_json(orient='table')}
        pprint(output)
        
def leftOuterJoin(key, left_table, right_table):
    customers_df = pd.read_csv('customers.csv')
    orders_df = pd.read_csv('orders.csv')
    if key == 'cid':
        orders_df.columns = ['oid','cid','name']
        print ("For key : ", key)
        leftOuterJoin      = pd.merge(customers_df,orders_df, on=key,how = 'left')
        rowsSkippedLeft = len(left_table) - len(leftOuterJoin.drop_duplicates('cid'))
        rowsSkippedRight = len(right_table) - len(leftOuterJoin.drop_duplicates('cid'))
        output = {'Result Count': len(leftOuterJoin), 'Rows Skipped on Left': rowsSkippedLeft  ,'Rows Skipped on Right': rowsSkippedRight, 'Result': leftOuterJoin.to_json(orient='table')}
        pprint(output) 
    elif key == 'customer_id':
        customers_df.columns = ['customer_id','name']
        print ('For key : ', key)
        leftOuterJoin      = pd.merge(customers_df,orders_df, on=key,how = 'left')
        rowsSkippedLeft = len(left_table) - len(leftOuterJoin.drop_duplicates('customer_id'))
        rowsSkippedRight = len(right_table) - len(leftOuterJoin.drop_duplicates('customer_id'))
        output = {'Result Count': len(leftOuterJoin), 'Rows Skipped on Left': rowsSkippedLeft  ,'Rows Skipped on Right': rowsSkippedRight, 'Result': leftOuterJoin.to_json(orient='table')}
        pprint(output)  

def rightOuterJoin(key, left_table, right_table):
    customers_df = pd.read_csv('customers.csv')
    orders_df = pd.read_csv('orders.csv')
    if key == 'cid':
        orders_df.columns = ['oid','cid','name']
        print ("For key : ", key)
        rightOuterJoin      = pd.merge(customers_df,orders_df, on=key,how = 'right')
        rowsSkippedLeft = len(left_table) - len(rightOuterJoin.drop_duplicates('cid'))
        rowsSkippedRight = len(right_table) - len(rightOuterJoin.drop_duplicates('cid'))
        output = {'Result Count': len(rightOuterJoin), 'Rows Skipped on Left': rowsSkippedLeft  ,'Rows Skipped on Right': rowsSkippedRight, 'Result': rightOuterJoin.to_json(orient='table')}
        pprint(output) 
    elif key == 'customer_id':
        customers_df.columns = ['customer_id','name']
        print ('For key : ', key)
        rightOuterJoin      = pd.merge(customers_df,orders_df, on=key, how = 'right')
        rowsSkippedLeft = len(left_table) - len(rightOuterJoin.drop_duplicates('customer_id'))
        rowsSkippedRight = len(right_table) - len(rightOuterJoin.drop_duplicates('customer_id'))
        output = {'Result Count': len(rightOuterJoin), 'Rows Skipped on Left': rowsSkippedLeft  ,'Rows Skipped on Right': rowsSkippedRight, 'Result': rightOuterJoin.to_json(orient='table')}
        pprint(output)  

p = argparse.ArgumentParser()
p.add_argument('join', default= 'inner')
p.add_argument('left_table', default= 'customers.json')
p.add_argument('right_table', default= 'orders.json')
p.add_argument('key', default='customer_id')
args = p.parse_args()
if __name__ == '__main__':

    left_table = loadJSON(args.left_table)
    right_table = loadJSON(args.left_table)
    json2csv('customers.json','orders.json')
    if args.join == 'inner':
        innerJoin(args.key,left_table, right_table)
    elif args.join == 'left_outer':
        leftOuterJoin(args.key,left_table, right_table)
    elif args.join == 'right_outer':
        rightOuterJoin(args.key,left_table, right_table)
