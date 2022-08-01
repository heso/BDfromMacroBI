__all__ = ['get_deals']

import datetime
import decimal
import json

from datetime import datetime as dt
from loguru import logger

from ..configs import captions_Deals, url_deals, host, username, password, database
from ..postgreSQL import MacroBIDB, ConnectionDBError, SQLError
from .usefull_functions import get_json_from_url


def get_deals_data(date_from: str):

    url = f'{url_deals}?date_modified_from={date_from}'
    json_deals = get_json_from_url(url)

    flag_running = len(json_deals['data']) != 0
    date_progress = (dt.strptime(date_from, "%d.%m.%Y")).date()
    data = []

    while flag_running:
        for deal in json_deals['data']:
            deal_id = _get_id(deal)
            house_id = _get_house_id(deal)
            agreement_date = _get_agreement_date(deal)
            date_modified = _get_date_modified(deal)
            status_modified_date = date_modified
            is_payed_reserve = _get_is_payed_reserve(deal)
            area = _get_area(deal)
            deal_sum = _get_deal_sum(deal)
            status = _get_status(deal)
            category = _get_category(deal)
            mediator_commission = _get_mediator_commission(deal)
            agent = _get_agent(deal)
            bank,  bank_name = _get_bank(deal)
            program = _get_program(deal)
            flat_number = _get_flat_number(deal)

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
                         is_payed_reserve,
                         deal_sum,
                         bank,
                         bank_name,
                         program,
                         agent,
                         mediator_commission,
                         house_id,
                         flat_number))

        if json_deals['next'] is None:
            flag_running = False
        else:
            url_new = url + '&from=' + str(json_deals['next'])
            json_deals = get_json_from_url(url_new)
    return data


def _get_id(deal: json) -> int:
    return deal['id']


def _get_house_id(deal: json) -> int:
    return deal['object']['parent_id']


def _get_agreement_date(deal: json) -> datetime.date:
    result = None
    if deal['deal']['agreement_date']:
        result = dt.strptime(deal['deal']['agreement_date'], '%d.%m.%Y').date()
    return result


def _get_date_modified(deal: json) -> datetime.date:
    return dt.strptime(deal['deal']['date_modified'], '%Y-%m-%d %H:%M:%S').date()


def _get_is_payed_reserve(deal: json) -> int:
    return deal['deal']['is_payed_reserve']


def _get_area(deal: json) -> int:
    return deal['object']['estate_area']


def _get_deal_sum(deal: json) -> decimal:
    return deal['deal']['deal_sum']


def _get_status(deal: json) -> int:
    return deal['deal']['status']


def _get_category(deal: json) -> str:
    return deal['object']['category']


def _get_mediator_commission(deal: json) -> int:
    return deal['deal']['deal_mediator_comission']


def _get_flat_number(deal: json) -> str:
    return deal['object']['geo_flatnum']


def _get_agent(deal: json) -> str:
    result = 'Не определено'
    if deal['agent'] is not None:
        result = deal['agent']['agent_org']['name']
        if result == 'Null' or result == '' or result is None:
            result = deal['agent']['name']
    return result


def _get_bank(deal: json) -> [bool, str]:
    bank = False
    bank_name = 'Не заполнено'
    if deal['bank'] is not None:
        bank = True
        bank_name = deal['bank']['name']
    return [bank, bank_name]


def _get_program(deal: json) -> str:
    result = ''
    if deal['deal'] is not None:
        result = deal['deal']['deal_program']
    return result


@logger.catch()
def get_deals():
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
    get_deals()
