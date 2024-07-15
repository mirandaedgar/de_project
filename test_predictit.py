import json
import requests
import os
import pandas as pd

def json_scraper(url, file_name):
    print('starting...')
    response = requests.request("GET", url)
    json_data=response.json()

    ##save extract in repo
    with open(os.getcwd() + file_name, 'w', encoding='utf-8') as json_file:
        json.dump(json_data, json_file, ensure_ascii=False, indent=4)

    print('complete')

def json_parser(json_path, file_path):

    with open(os.getcwd() + json_path) as json_file:    
        data = json.load(json_file)

    df = pd.json_normalize(data, 'markets')
    df.to_csv(os.getcwd() + file_path + '\markets.csv', index=False)
    
    df_c = pd.json_normalize(data['markets'], 'contracts', ['id'], 
                    record_prefix='contracts_', errors='ignore')
    df_c.to_csv(os.getcwd() + file_path+ '\contracts.csv', index=False)

json_scraper('https://www.predictit.org/api/marketdata/all/', '\Documents\Github\predictit_project\predictit_market.json')
json_parser('\Documents\Github\predictit_project\predictit_market.json', '\Documents\Github\predictit_project')