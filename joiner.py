import json
import os
from pprint import pprint

def loadJSON(filename):
    with open(filename) as data_file:    
        data = json.load(data_file)
    # pprint(data)
    return data

def innerJoin(left_table, right_table):
    left = loadJSON(left_table)
    right = loadJSON(right_table)
    results = []
    for i, left_entry in enumerate(left):
        for j, right_entry in enumerate(right):
            if left_entry.get('cid') == right_entry.get('customer_id'):
                # print(left_entry.get('cid'))
                # left_entry.pop('cid')
                right_entry.pop('customer_id')
                left_entry.update(right_entry)
                # print(left_entry)
                results.append(left_entry)
    for k, result in enumerate(results):
        if 'cid' in result: 
            del result['cid']
    pprint(results)

def left_outerJoin(left_table, right_table):
    pass



if __name__ == '__main__':

    # loadJSON('customers.json')
    # loadJSON('orders.json')
    innerJoin('customers.json', 'orders.json')
