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
    '''
    Get all ids from Notion for project C6274.
    :return dict: dictionary containing codes, status and hours downloaded from Clockify.
    '''
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
        try:
            id_notion = row['properties']['ID']['title'][0]['plain_text']
            id_status = row['properties']['Status planos']['status']['name']
            id_page = row['id']

            try:
                id_comment_date = row ['properties']['Llegada comentarios']['date']['start']
            except:
                id_comment_date = ''
            
            ids.append(id_notion)
            ids_status.append(id_status)
            ids_comments_date.append(id_comment_date)
            ids_pages.append(id_page)

        except:
            pass

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
            try:
                id_notion = row['properties']['ID']['title'][0]['plain_text']
                id_status = row['properties']['Status planos']['status']['name']
                id_page = row['id']

                try:
                    id_comment_date = row ['properties']['Llegada comentarios']['date']['start']
                except:
                    id_comment_date = ''
                
                ids.append(id_notion)
                ids_status.append(id_status)
                ids_comments_date.append(id_comment_date)
                ids_pages.append(id_page)

            except:
                pass

    return {'code':ids, 'status':ids_status, 'comment_date':ids_comments_date, 'id_page':ids_pages}

def transform_data_excel(excel_file):

    df = pd.read_excel(excel_file, engine='openpyxl')
    df.fillna('', inplace=True)
    df.replace({np.nan: None}, inplace = True)

    codes = []
    dates_recive = []
    zones = []
    areas = []
    descriptions = []
    status_comments = []
    reject_comments = []

    for dcm_number,dcm_document,date_recive,description, status_comment, reject_comment in zip(
        df['DCM Number'],
        df['Document Number'],
        df['Supplier Involvment Date'],
        df['Description'],
        df['Sketch Status'],
        df['Rejection Note']):

        code = f'DCM{dcm_number[-4:]}-{dcm_document[6:]}'
        zone= dcm_document[12:14]
        area = dcm_document[10:12]
        if not date_recive is None:
            date_recive =  dates_format(date_recive)
        else:
            date_recive = ''

        codes.append(code)
        dates_recive.append(date_recive)
        zones.append(zone)
        areas.append(area)
        descriptions.append(description)
        status_comments.append(status_comment)
        reject_comments.append(reject_comment)

    excel = {'code':codes, 'date_recive':dates_recive,'zone':zones,'area':areas,'description':descriptions,'status_comment':status_comments, 'reject_comment':reject_comments}
    
    df = pd.DataFrame(data=excel)

    return df

def upload_new_pages(df_excel, notion_data:list):
    
    headers={
        'Notion-Version': notion_version,
        'Content-Type': content,
        'Authorization': f'Bearer {api_key_notion}'
    }

    for index,row in df_excel.iterrows():
        
        if row['code'] in notion_data:
            continue

        url = 'https://api.notion.com/v1/pages'
        data = {
            'parent':{'database_id':f'{db_id}'},
            'properties': {
                'ID': {
                    'title': [{'text': {'content': row['code']}}]
                },
                'Zona': {
                    'select': {'name': row['zone']}
                },
                'Area': {
                    'select': {'name': row['area']}
                },
                'Descripción': {
                    'rich_text': [{'text': {'content': row['description']}}]
                },
                'Status modelo': {
                    'status': {'name': 'Back-log'}
                },
            }
        }
    
        if not row['date_recive'] == '':
            data['properties']['Fecha de llegada'] = {'date': {'start': row['date_recive']}}
        
        response = requests.post(url=url,headers=headers, data=json.dumps(data))


def update_pages_with_comments(df_excel,notion_data):
    
    df_excel = df_excel[df_excel['status_comment']=='Rejected']
    df_notion = pd.DataFrame(data=notion_data)
    df=pd.merge(df_notion, df_excel, on='code')

    headers={
        'Notion-Version': notion_version,
        'Content-Type': content,
        'Authorization': f'Bearer {api_key_notion}'
    }

    # Importante solo se suben las que no tienen fecha de llegada de comentarios en Notion, lo que implica que si cambia el comentario en Cruscotto no se actualiza en Notion.


    for page,comment,comment_date in zip(df['id_page'],df['reject_comment'],df['comment_date']):

        if comment_date == "":

            now = datetime.today()

            body ={
                'id': page,
                'properties':{
                    'Comentarios': {'rich_text': [{'text': {'content': comment}}]},
                    'Llegada comentarios':{'date': {'start': now.strftime('%Y-%m-%d') }}
                }
            }
            url = f'https://api.notion.com/v1/pages/{page}'

            response = requests.patch(url, headers = headers , data = json.dumps(body))

            print(response.status_code)




notion_data = get_ids()

for path in os.listdir(os.getcwd()):
    if os.path.isfile(os.path.join(os.getcwd(),path)):
        try:
            file_name = path.split('_')
            file_type = file_name[1].split('.')[0]
            file_extension = file_name[1].split('.')[1]
        except:
            file_type = ''
            file_extension = ''
        
        if file_type == 'DCMs' and file_extension == 'xlsx':
           df_excel = transform_data_excel(excel_file=path)
           upload_new_pages(df_excel=df_excel,notion_data=notion_data['code'])
           update_pages_with_comments(df_excel=df_excel, notion_data=notion_data)







