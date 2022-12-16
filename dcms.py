import pandas as pd
import numpy as np
import os
import requests
import json

from datetime import datetime


api_key_notion = 'secret_y8g2Ij4kwAO7GD6jR7kH7OVn4nPTfGguIG8CxxNQJf9'
api_key_clockify = 'NjVmNTU1N2MtZThmNy00ZTQyLTk1ZDYtMjU2NjQ0ZmJjMDdk'
db_id = '4a4d0b4e45ca495783104ab5321e2bbe'
clockify_client_id = '630db8a1b59c366b0e2ba9a7'
notion_version = '2022-06-28'
content = 'application/json'


def intro():
    print('\n')
    print(datetime.today())
    print('Project: C.6274')
    print('Current version 1.0.0')
    print('In case of any error please contact JLC by email (jose.conde@ghenova.net) \n')

    return None

def dates_format(value):
    return datetime.strftime(value, '%Y-%m-%d')


def get_ids()->dict:
    """
    Get all ids from Notion for project C6274.
    :return dict: dictionary containing codes, status and hours downloaded from Clockify.
    """
    headers={
        'Notion-Version': notion_version,
        'Content-Type': content,
        'Authorization': f'Bearer {api_key_notion}'
    }

    body = {
        'sorts': [{'property': 'ID','direction': 'ascending'}]
    }

    url = f'https://api.notion.com/v1/databases/{db_id}/query'
    response = requests.post(url=url,headers=headers,  data = json.dumps(body))

    if response.status_code != 200:
        raise Exception() 

    response_json = json.loads(response.text)

    ids = []
    ids_status = []
    ids_comments_date = []
    ids_pages = []

    for row in response_json['results']:
        id_notion = row['properties']['ID']['title'][0]['plain_text']
        id_status = row['properties']['Status planos']['status']['name']
        id_page = row['id']

        try:
            id_comment_date = row ['properties']["Llegada comentarios"]["date"]["start"]
        except:
            id_comment_date = ""

        ids.append(id_notion)
        ids_status.append(id_status)
        ids_comments_date.append(id_comment_date)
        ids_pages.append(id_page)

    while response_json['has_more']:

        body ={
            'start_cursor':str(response_json['next_cursor']),
            'sorts': [{'property': 'ID','direction': 'ascending'}]
        }

        response = requests.post(url, headers = headers , data = json.dumps(body))

        if response.status_code != 200:
            raise Exception()
            
        response_json = json.loads(response.text)

        for row in response_json['results']:
            
            id_notion = row['properties']['ID']['title'][0]['plain_text']
            id_page = row['id']

            try:
                id_comment_date = row ['properties']["Llegada comentarios"]["date"]["start"]
            except:
                id_comment_date = ""

            ids.append(id_notion)
            ids_status.append(id_status)
            ids_comments_date.append(id_comment_date)
            ids_pages.append(id_page)

    print(ids_comments_date)

    return {'codes':ids, 'status':ids_status, 'comments_date':ids_comments_date, 'ids_pages':ids_pages}



def upload_new_dcms(file,ids:list):
    
    headers={
        'Notion-Version': notion_version,
        'Content-Type': content,
        'Authorization': f'Bearer {api_key_notion}'
    }

    dcms = 0

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
            
            #response = requests.post(url=url,headers=headers, data=json.dumps(data))
            
            dcms += 1 

            #print(f'Status response:{response.status_code}/{dcms}')
        
    return dcms

def update_dcms(df_excel,ids:list):

    headers={
        'Notion-Version': notion_version,
        'Content-Type': content,
        'Authorization': f'Bearer {api_key_notion}'
    }

    df_excel = df_excel[df_excel['Sketch Status']=='Rejected']
    
    df_excel = df_excel.loc[:, ['codes', 'status','hours']]
    df_notion = pd.DataFrame(data=ids)

    df=pd.merge(df_notion, df_excel, on='codes')

    print(df)

    """
    for dcm_number,dcm_document,status,comment in zip(file['DCM Number'],file['Document Number'],file['Sketch Status'],file['Sketch Status'],file['Rejection Note']):

        id = f'DCM{dcm_number[-4:]}-{dcm_document[6:]}'

        if id in ids:
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
            
            #response = requests.post(url=url,headers=headers, data=json.dumps(data))
            
            dcms += 1 

            #print(f'Status response:{response.status_code}/{dcms}')
        
    return dcms
    """





ids = get_ids()

print(ids)


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

            df = pd.read_excel(path, engine='openpyxl')
            df.fillna("", inplace=True)
            df.replace({np.nan: None}, inplace = True)

            update_dcms(df, ids)








