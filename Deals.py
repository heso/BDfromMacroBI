import requests
import json
import os
import urllib.parse
from dotenv import load_dotenv

from datetime import datetime as dt
from DB_Config import captions_Deals, api_url
from PostgreSQL import *

load_dotenv()


def get_data(date_from: str):
    api_key = urllib.parse.quote_plus(os.environ.get('api_key'))

    url = f'{api_url}{api_key}/getEstateDeals.json?agreement_date_from={date_from}'

    request = requests.get(url).content
    json_deals = json.loads(request)

    flag_running = len(json_deals['data']) != 0

    date_progress = (dt.strptime(date_from, "%d.%m.%Y")).date()
    data = []

    while flag_running:
        for deal in json_deals['data']:
            deal_id = deal['id']
            house_id = deal['object']['parent_id']
            agreement_date = dt.strptime(deal['deal']['agreement_date'], '%d.%m.%Y').date()
            area = deal['object']['estate_area']
            deal_sum = deal['deal']['deal_sum']
            category = deal['object']['category']
            mediator_commission = deal['deal']['deal_mediator_comission']

            agent = ''
            if deal['agent'] is not None:
                agent = deal['agent']['agent_org']['name']
                if agent == 'Null' or agent == '' or agent is None:
                    agent = deal['agent']['name']

            bank = False
            bank_name = ''
            if deal['bank'] is not None:
                bank = True
                bank_name = deal['bank']['name']

            program = ''
            if deal['deal'] is not None:
                program = deal['deal']['deal_program']

            if agreement_date > date_progress:
                date_progress = agreement_date
                print(date_progress)

            data.append((deal_id,
                         agreement_date,
                         area,
                         category,
                         deal_sum,
                         bank,
                         bank_name,
                         program,
                         agent,
                         mediator_commission,
                         house_id))

        if json_deals['next'] is None:
            flag_running = False
        else:
            url_new = url + '&from=' + str(json_deals['next'])
            request = requests.get(url_new).content
            json_deals = json.loads(request)
    return data


def main():
    host = os.environ.get('db_host')
    username = os.environ.get('db_username')
    password = os.environ.get('db_password')
    database = os.environ.get('db_base')

    try:
        with MacroBIDB(host, username, password, database) as db:
            db.create_table('Deals', captions_Deals, True)
            date_from = dt.strftime(db.get_maximum_date('Deals'), '%d.%m.%Y')
            data = get_data(date_from)
            db.insert_data('Deals', captions_Deals, data, True)
    except ConnectionError as err:
        print(f'Unable to connect to DB {str(err)}')
    except SQLError as err:
        print(f'Something wrong with query: {str(err)}')
    except Exception as err:
        print(f'Something went wrong: {str(err)}')


if __name__ == '__main__':
    main()
