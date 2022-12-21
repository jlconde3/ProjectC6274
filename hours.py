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
    print('Current version 1.1.0')
    print('In case of any error please contact JLC by email (jose.conde@ghenova.net) \n')
    return None


def get_notion_data()->dict:
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
    ids_pages = []

    for row in response_json['results']:
        try: 
            id_notion = row['properties']['ID']['title'][0]['plain_text']
            id_status = row['properties']['Status planos']['status']['name']
            id_page = row['id']

            ids.append(id_notion)
            ids_status.append(id_status)
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
                ids.append(id_notion)
                ids_status.append(id_status)
                ids_pages.append(id_page)

            except: 
                pass


    return {'codes':ids, 'status':ids_status,'ids_pages':ids_pages}


def format_date(date):
    date_mod = str(date).split(' ')
    return date_mod[0] + 'T' + date_mod[1]


def get_time_record (init_date,end_date, ids_clockify:list, hours_clockify:list)->int:
    """
    Get time records from Clockify for a specific day.
    :param init_date: initial date of query.
    :param end_date: final date of query.
    :param ids_clockify: list of dcms downloaded from Clockify.
    :param hours_clockify: list of hours downloaded from Clockify.
    :return int: response status after connect clockify api.
    """
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

        ids_clockify.append(id)
        hours_clockify.append(task_consumed_hours)

    return response.status_code


def get_all_time_records()->dict:
    """
    Get all time records from Clockify for project C6274 from 01/12/2022 to today.
    :return dict: dictionary containing codes and hours downloaded from Clockify.
    """

    last_date = datetime.strptime(datetime.today().strftime('%Y-%m-%d'), '%Y-%m-%d') + timedelta(days=1)
    init_date = datetime(2022,12,1)

    codes = []
    hours = []

    while init_date != last_date:
        end_date = init_date + timedelta(days=1)

        get_time_record(init_date=format_date(init_date),end_date=format_date(end_date), ids_clockify= codes, hours_clockify=hours)

        init_date += timedelta(days=1)
    
    return {'codes':codes, 'hours':hours }


def process_data(clockify_data:dict, notion_data:dict):

    df_clockify = pd.DataFrame(data=clockify_data)
    df_clockify2= df_clockify.groupby(['codes']).sum()
    df_notion = pd.DataFrame(data=notion_data)
    df=pd.merge(df_notion, df_clockify2, on='codes')

    return df


def upload_hours_to_notion(df)->int:

    pages_update = 0

    headers = {
        'Authorization': f'Bearer {api_key_notion}',
        'Content-Type': 'application/json',
        'Notion-Version': '2022-06-28'
    }

    for page,hours in zip(df['ids_pages'],df['hours']):

        body ={
            'id': page,
            'properties':{'Horas reales': {'number': round(hours,2)}}
        }
        url = f'https://api.notion.com/v1/pages/{page}'
        response = requests.patch(url, headers = headers , data = json.dumps(body))

        if response.status_code != 200:
            raise Exception() 

        pages_update += 1

    return pages_update
    



def generate_csv(df):
    """
    Gnerete a csv file with all DCMs in DCM Delivered status.
    :param df: dataframe with data from merge dataframes.
    :return None:
    """
    df = df[df['status']=='DCM Delivered']
    df2 = df.loc[:, ['codes', 'status','hours']]
    path = os.path.join(os.getcwd(),'data.csv')
    df2.to_csv(path_or_buf=path, sep=';')


def main():
    
    intro()

    print('Loading results from Notion')
    try:
        notion_data = get_notion_data()
        print('Success!!')
    except:
        print('An error occurred while loading data from Notion... please contact support :(')
        return None
    
    print('Loading results from Clockify')
    try:
        clockify_data = get_all_time_records()
        print('Success!!')
    except:
        print('An error occurred while loading data from Clockify... please contact support :(')
        return None

    print('Processing data...')
    try:
        df = process_data(notion_data=notion_data, clockify_data=clockify_data)
        print('Success!!')
    except:
        print('An error occurred while processing data... please contact support :(')
        return None

    print('Updating hours to Notion')
    try:
        pages_update = upload_hours_to_notion(df)
        print('{pages} pages update to Notion '.format(pages = pages_update))
        print('Success!!')
    except:
        print('An error occurred while updating Notion... please contact support :(')
        return None
    
    print('Generating csv file')
    try:
        generate_csv(df)
        print('Success!!')
    except:
        print('An error occurred while creating the csv file... please contact support :(')
        return None

    return 200


if __name__ == "__main__": 
    main()