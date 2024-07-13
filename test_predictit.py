import json
import requests

def json_parser(url, file_name, bucket):
    print('starting...')
    response = requests.request("GET", url)
    json_data=response.json()

    with open(file_name, 'w', encoding='utf-8') as json_file:
        json.dump(json_data, json_file, ensure_ascii=False, indent=4)

    print('complete')
    #s3 = boto3.client('s3')
    #s3.upload_file(file_name, bucket,f"predictit/{file_name}")

json_parser('https://www.predictit.org/api/marketdata/all/', 'predictit_market.json', 'data-mbfr')