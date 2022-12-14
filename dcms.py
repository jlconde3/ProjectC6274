import pandas as pd
import numpy as np
import os
import requests
import json

from datetime import datetime


api_key = "secret_y8g2Ij4kwAO7GD6jR7kH7OVn4nPTfGguIG8CxxNQJf9"
db_id = "4a4d0b4e45ca495783104ab5321e2bbe"
notion_version = "2022-06-28"
content = "application/json"

headers={
    'Notion-Version': notion_version,
    'Content-Type': content,
    'Authorization': f'Bearer {api_key}'}


def get_ids():

    body = {
        "sorts": [{"property": "ID","direction": "ascending"}]
    }
    url = f'https://api.notion.com/v1/databases/{db_id}/query'
    response = requests.post(url=url,headers=headers,  data = json.dumps(body))
    response_json = json.loads(response.text)

    ids = []

    for row in response_json['results']:
        ids.append(row['properties']['ID']['title'][0]['plain_text'])

        
    while response_json["has_more"]:
        body ={
            "start_cursor":str(response_json["next_cursor"]),
            "sorts": [{"property": "ID","direction": "ascending"}]
        }

        response = requests.post(url, headers = headers , data = json.dumps(body) )
        response_json = json.loads(response.text)

        for row in response_json['results']:
            ids.append(row['properties']['ID']['title'][0]['plain_text'])
        
    return ids


def dates_format(value):
    return datetime.strftime(value, '%Y-%m-%d')


def get_parms(file,ids:list):
    ncs = 0
    for dcm_number,dcm_document,date_recive,description in zip(
        file['DCM Number'],file['Document Number'],
        file['Supplier Involvment Date'],
        file['Description']):

        id = f'DCM{dcm_number[-4:]}-{dcm_document[6:]}'
        if not id in ids:

            url = "https://api.notion.com/v1/pages"
            data = {
                'parent':{'database_id':f'{db_id}'},
                "properties": {
                    "ID": {
                        "title": [{"text": {"content": id}}]
                    },
                    "Zona": {
                        "select": {"name": dcm_document[12:14]}
                    },
                    "Area": {
                        "select": {"name": dcm_document[10:12]}
                    },
                    "Descripción": {
                        "rich_text": [{"text": {"content": description}}]
                    },
                    "Status modelo": {
                        "status": {"name": "Back-log"}
                    },
                }
            }
        
            if not date_recive is None:
                data['properties']['Fecha de llegada'] = {"date": {"start": dates_format(date_recive)}}
            
            response = requests.post(url=url,headers=headers, data=json.dumps(data))
            
            ncs += 1 

            print(f'Status response:{response.status_code}/{ncs}')
        


    return ncs



print('PMS APP NOTION API')
print(datetime.today())
print('Project: C6274')
print("Executing automatic upload of DCMs to Notion...")
print('Reading files from directory')
print('Getting IDs from Notion...')
ids = get_ids()
print(f'Loaded {len(ids)} IDs from Notion')


for path in os.listdir(os.getcwd()):
    if os.path.isfile(os.path.join(os.getcwd(),path)):
        try:
            file_name = path.split('_')
            file_type = file_name[1].split('.')[0]
            file_extension = file_name[1].split('.')[1]
        except:
            file_type = ""
            file_extension = ""
        
        if file_type == 'DCMs' and file_extension == 'xlsx':

            file = pd.read_excel(path, engine='openpyxl')
            print(f'Reading {file_name}')
            print('Cleaning data...')
            file.fillna("", inplace=True)
            file.replace({np.nan: None}, inplace = True)
            
            total = get_parms(file,ids)

print(f'Loaded {total} new IDs to Notion')


