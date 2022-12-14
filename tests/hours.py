import requests
import json
import pandas as pd
import os

from datetime import datetime, timedelta

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


def get_ids():
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
    ids_pages = []

    for row in response_json['results']:
        status_notion = row['properties']['Status planos']['status']['name'] 
        id_notion = row['properties']['ID']['title'][0]['plain_text']
        id_page = row['id']

        if status_notion == 'DCM Delivered':
            ids.append(id_notion)
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
            status_notion = row['properties']['Status planos']['status']['name'] 
            id_notion = row['properties']['ID']['title'][0]['plain_text']

            if status_notion == 'DCM Delivered':
                ids.append(id_notion)
                ids_pages.append(id_page)
    
    return {'codes':ids, 'ids_pages':ids_pages}


def format_date(date):
    date_mod = str(date).split(' ')
    return date_mod[0] + 'T' + date_mod[1]

def get_time (init_date,end_date, ids_notion, id_array:list, hours_array:list):
    headers= {
        'X-Api-Key': api_key_clockify ,
        'content-type':content
    }
    
    request_payload = { 
        'dateRangeStart':init_date,
        'dateRangeEnd': end_date,
        'detailedFilter': {'page': '1','pageSize': '1000'},
        'clients': {
            'ids': [clockify_client_id],
            'contains': 'CONTAINS',
            'status': 'ALL'
        }
    }
    
    url = f'https://reports.api.clockify.me/v1/workspaces/62ea6cd620a7ae4e94af3a6b/reports/detailed'
    response = requests.post(url, headers= headers, json=request_payload)

    if response.status_code != 200:
        raise Exception() 

    response_json = json.dumps(response.json(), indent=4)
    data_clockify = json.loads(response_json)


    for i in data_clockify['timeentries']:

        id = i['description']
        task_consumed_hours = round (float (i['timeInterval']['duration'])/3600,2)

        if id in ids_notion:
            id_array.append(id)
            hours_array.append(task_consumed_hours)

    return response.status_code


def get_all_time_records(notion_data):

    last_date = datetime.strptime(datetime.today().strftime('%Y-%m-%d'), '%Y-%m-%d') + timedelta(days=1)
    init_date = datetime(2022,12,1)

    codes = []
    hours = []

    while init_date != last_date:
        end_date = init_date + timedelta(days=1)

        get_time(init_date=format_date(init_date),end_date=format_date(end_date), ids_notion=notion_data['codes'], id_array= codes, hours_array=hours)

        init_date += timedelta(days=1)
    
    return {'codes':codes, 'hours':hours }


def upload_hours_to_notion(dataframe):
    headers = {
        'Authorization': f'Bearer {api_key_notion}',
        'Content-Type': 'application/json',
        'Notion-Version': '2022-06-28'
    }

    for page,hours in zip(dataframe['ids_pages'],dataframe['hours']):

        body ={
            'id': page,
            'properties':{'Horas reales': {'number': hours}}
        }
        url = f'https://api.notion.com/v1/pages/{page}'
        response = requests.patch(url, headers = headers , data = json.dumps(body))

        if response.status_code != 200:
            raise Exception() 


def main():
    
    intro()

    print('Loading results from Notion')
    try:
        notion_data = get_ids()
        print('Success!!')
    except:
        print('An error occurred while loading data from Notion... please contact support :(')
        return None

    print('Loading results from Clockify')
    try:
        clockify_data = get_all_time_records(notion_data=notion_data)
        print('Success!!')
    except:
        print('An error occurred while loading data from Clockify... please contact support :(')
        return None

    print('Uploading results to Notion')
    try:
        df_clockify = pd.DataFrame(data=clockify_data)
        df_clockify = df_clockify.groupby(['codes']).sum()
        df_notion = pd.DataFrame(data=notion_data)
        df=pd.merge(df_notion, df_clockify, on='codes')
        upload_hours_to_notion(df)
        print('Success!!')
    except:
        print('An error occurred while updating Notion... please contact support :(')
        return None
    
    print('Generating csv file')
    try:
        path = os.path.join(os.getcwd(),'data.csv')
        df_clockify.to_csv(path_or_buf=path, sep=';')
        print('Success!!')
    except:
        print('An error occurred while creating the csv file... please contact support :(')
        return None

    return 200


if __name__ == "__main__": 
    main()