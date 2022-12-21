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
    print('Project: C6274')
    print('Current version 1.1.1')
    print('In case of any error please contact JLC by email (jose.conde@ghenova.net) \n')

    return None

def get_excel():
    '''
    Look for excel file with new infrmation from Crusscotto.

    :return path:string path to excel file.
    '''
    for path in  os.listdir(os.getcwd()):
        if os.path.isfile(os.path.join(os.getcwd(),path)):
            file_extension =path.rsplit('.',1)[1]
            if file_extension == 'xlsx':
                return path

    raise Exception()


def dates_format(date)->str:
    '''
    Format date to string.

    :param date: datetime object to convert to str.
    :return string: with date format.
    '''

    return datetime.strftime(date, '%Y-%m-%d')


def get_notion_data()->dict:
    '''
    Get all ids from Notion for project C6309.

    :return dict: dictionary containing codes, status_modelo,comment date, and pages id from Notion.
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


def transform_data_excel(excel_file:str):

    '''
    Transform data stored in excel to pandas Dataframe.

    :param excel_file: path to excel file.
    :return df: pandas dataframe.
    '''

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

    for dcm_number,dcm_document,date_recive,description,status, status_comment, reject_comment in zip(
        df['DCM Number'],
        df['Document Number'],
        df['Supplier Involvment Date'],
        df['Description'],
        df['Closure'],
        df['Sketch Status'],
        df['Rejection Note']):

        if status in ['Sketch sent to Fincantieri','Involvement to Do','Awaiting Info FC'] and not status_comment in ['Approved', 'Under Approval']:

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
    
    '''
    Upload new pages to Notion with the new entries found from excel file.

    :param df_excel: dataframe form excel file with new info. 
    :param notion_data: list with all pages downloaded from Notion.
    :return pages_upload: total count of pages upload to Notion.
    '''
    
    pages_upload = 0

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
        
        pages_upload += 1

    return pages_upload


def update_pages_with_comments(df_excel,notion_data):
    '''
    Update pages in Notion. Only udpate the ones that have been rejected.
    Update field comment with value from excel and today date in Notion.

    :param df_excel: dataframe form excel file with new info. 
    :param notion_data: dict with all page info downloaded from Notion.
    :return pages_update: total count of pages update to Notion.
    '''

    pages_update = 0
    
    df_excel = df_excel[df_excel['status_comment']=='Rejected']
    df_notion = pd.DataFrame(data=notion_data)
    df=pd.merge(df_notion, df_excel, on='code')

    headers={
        'Notion-Version': notion_version,
        'Content-Type': content,
        'Authorization': f'Bearer {api_key_notion}'
    }

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

            pages_update +=1
    
    return pages_update

    
def main():

    intro()

    print('Searching for Excels files...')
    try:
        path = get_excel()
        print('Success!!')
    except:
        print('An error occurred while searching for an excel file... please contact support :(')
        return None

    print(f'Loading data from {path}')
    try:
        df_excel =transform_data_excel(excel_file=path)
        print('Success!!')
    except:
        print('An error occurred while loading excel file... please contact support :(')
        return None

    print('Loading results from Notion')
    try:
        notion_data = get_notion_data()
        print('Success!!')
    except:
        print('An error occurred while loading data from Notion... please contact support :(')
        return None

    print('Processing data from excel file with Notion..')
    print('Uploading new pages to Notion...')
    try:
        pages_upload = upload_new_pages(df_excel=df_excel,notion_data=notion_data['code'])

        print('{pages} pages upload to Notion '.format(pages = pages_upload))
        print('Success!!')
    except:
        print('An error occurred while uploading new pages to Notion... please contact support :(')
        return None

    print('Updating new pages...')
    try:
        pages_update = update_pages_with_comments(df_excel=df_excel, notion_data=notion_data)

        print('{pages} pages update to Notion '.format(pages = pages_update))
        print('Success!!')
    except:
        print('An error occurred while updating Notion... please contact support :(')
        return None

    return 200


if __name__ == "__main__": 
    main()