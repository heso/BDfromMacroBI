import requests
import json
import os
import urllib.parse

from datetime import datetime as dt, timedelta
from dotenv import load_dotenv
from DB_Config import captions_Leads, api_url
from PostgreSQL import *

load_dotenv()


def get_data(date_from: str):

    api_key = urllib.parse.quote_plus(os.environ.get('api_key'))

    url = f'{api_url}{api_key}/getEstateBuys.json?date_modified_from={date_from}'

    request = requests.get(url).content
    json_leads = json.loads(request)

    flag_running = len(json_leads['data']) != 0

    date_progress = (dt.strptime(date_from, "%d.%m.%Y")).date()
    id_dict_list = dict()

    while flag_running:
        for lead in json_leads['data']:
            lead_id = lead['id']
            lead_date_added = lead['date_added']
            lead_status = lead['status']
            lead_category = None if lead['category'] == '' else lead['category']
            lead_date_added_dt = dt.fromtimestamp(lead_date_added).date()

            if lead_status in (30, 50, 100):
                lead_house_id = lead['sell_parent_id']
            else:
                if 'estate_buy_housesInterest' in lead['entity_attrs']:
                    lead_house_id = lead['entity_attrs']['estate_buy_housesInterest'][0]
                else:
                    lead_house_id = None

            lead_date_added_new = lead_date_added_dt
            if lead_date_added_new > date_progress:
                date_progress = lead_date_added_dt
                print(date_progress)

            if lead_id not in id_dict_list.keys():
                id_dict_list[lead_id] = (lead_id, lead_date_added_dt, lead_category, lead_status, lead_house_id)
            else:
                if lead_date_added_dt >= id_dict_list.get(lead_id)[1]:
                    id_dict_list[lead_id] = (lead_id, lead_date_added_dt, lead_category, lead_status, lead_house_id)

        if json_leads['next'] is None:
            flag_running = False
        else:
            url_new = url + '&from=' + str(json_leads['next'])
            request = requests.get(url_new).content
            json_leads = json.loads(request)

    return list(id_dict_list.values())


def main():
    host = os.environ.get('db_host')
    username = os.environ.get('db_username')
    password = os.environ.get('db_password')
    database = os.environ.get('db_base')

    try:
        with MacroBIDB(host, username, password, database) as db:
            db.create_table('Leads', captions_Leads, True)
            date_from = dt.strftime(db.get_maximum_date('Leads') - timedelta(1), '%d.%m.%Y')
            data = get_data(date_from)
            db.insert_data('Leads', captions_Leads, data, True)
    except ConnectionError as err:
        print(f'Unable to connect to DB {str(err)}')
    except SQLError as err:
        print(f'Something wrong with query: {str(err)}')
    except Exception as err:
        print(f'Something went wrong: {str(err)}')


if __name__ == '__main__':
    main()
