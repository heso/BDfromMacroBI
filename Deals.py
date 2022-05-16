import os
import json
import requests
import urllib.parse

from dotenv import load_dotenv
from datetime import datetime as dt
from loguru import logger

from DB_Config import captions_Deals, api_url
from PostgreSQL import MacroBIDB, ConnectionDBError, SQLError


load_dotenv()


def get_deals_data(date_from: str):
    api_key = urllib.parse.quote_plus(os.environ.get('api_key'))

    url = f'{api_url}{api_key}/getEstateDeals.json?date_modified_from={date_from}'

    request = requests.get(url).content
    json_deals = json.loads(request)

    flag_running = len(json_deals['data']) != 0

    date_progress = (dt.strptime(date_from, "%d.%m.%Y")).date()
    data = []

    while flag_running:
        for deal in json_deals['data']:
            deal_id = deal['id']
            house_id = deal['object']['parent_id']

            agreement_date = None
            if deal['deal']['agreement_date']:
                agreement_date = dt.strptime(deal['deal']['agreement_date'], '%d.%m.%Y').date()

            date_modified = dt.strptime(deal['deal']['date_modified'], '%Y-%m-%d %H:%M:%S').date()

            status_modified_date = date_modified

            area = deal['object']['estate_area']
            deal_sum = deal['deal']['deal_sum']
            status = deal['deal']['status']
            category = deal['object']['category']
            mediator_commission = deal['deal']['deal_mediator_comission']

            agent = 'Не определено'
            if deal['agent'] is not None:
                agent = deal['agent']['agent_org']['name']
                if agent == 'Null' or agent == '' or agent is None:
                    agent = deal['agent']['name']

            bank = False
            bank_name = 'Не заполнено'
            if deal['bank'] is not None:
                bank = True
                bank_name = deal['bank']['name']

            program = ''
            if deal['deal'] is not None:
                program = deal['deal']['deal_program']

            if date_modified > date_progress:
                date_progress = date_modified
                logger.info(f'Deals date - {date_progress}.')

            data.append((deal_id,
                         agreement_date,
                         date_modified,
                         status_modified_date,
                         area,
                         category,
                         status,
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


@logger.catch()
def main():
    host = os.environ.get('db_host')
    username = os.environ.get('db_username')
    password = os.environ.get('db_password')
    database = os.environ.get('db_base')

    try:
        with MacroBIDB(host, username, password, database) as db:
            db.create_table('Deals', captions_Deals, True)
            date_from = dt.strftime(db.get_maximum_date('Deals'), '%d.%m.%Y')
            data = get_deals_data(date_from)
            db.insert_data('Deals', captions_Deals, data, use_deals=True)
        logger.success('Deals updated.')
    except ConnectionDBError as err:
        logger.error(f'Deals. Unable to connect to DB {str(err)}')
    except SQLError as err:
        logger.error(f'Deals. Something wrong with query: {str(err)}')


if __name__ == '__main__':
    main()
