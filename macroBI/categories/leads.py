__all__ = ['get_leads']

import json

from datetime import datetime as dt, timedelta, date

from loguru import logger

from ..configs import captions_Leads, url_leads, host, username, password, database
from ..postgreSQL import MacroBIDB, ConnectionDBError, SQLError
from .usefull_functions import get_json_from_url


def get_leads_data(date_from: str):

    url = f'{url_leads}?date_modified_from={date_from}'
    json_leads = get_json_from_url(url)
    flag_running = len(json_leads['data']) != 0
    date_progress = (dt.strptime(date_from, "%d.%m.%Y")).date()
    id_dict_list = dict()

    while flag_running:
        for lead in json_leads['data']:
            lead_id = _get_id(lead)
            lead_status = _get_status(lead)
            lead_category = _get_category(lead)
            lead_date_added_dt = _get_date_added_dt(lead)
            lead_house_id = _get_house_id(lead)

            lead_date_added_new = lead_date_added_dt
            if lead_date_added_new > date_progress:
                date_progress = lead_date_added_dt
                logger.info(f'Leads date - {date_progress}.')

            if lead_id not in id_dict_list.keys():
                id_dict_list[lead_id] = (lead_id, lead_date_added_dt, lead_category, lead_status, lead_house_id)
            else:
                if lead_date_added_dt >= id_dict_list.get(lead_id)[1]:
                    id_dict_list[lead_id] = (lead_id, lead_date_added_dt, lead_category, lead_status, lead_house_id)

        if json_leads['next'] is None:
            flag_running = False
        else:
            url_new = url + '&from=' + str(json_leads['next'])
            json_leads = get_json_from_url(url_new)

    return list(id_dict_list.values())


def _get_id(lead: json) -> int:
    return lead['id']


def _get_date_added(lead: json) -> int:
    return lead['date_added']


def _get_status(lead: json) -> int:
    return lead['status']


def _get_category(lead: json) -> str:
    result = None if lead['category'] == '' else lead['category']
    return result


def _get_date_added_dt(lead: json) -> date:
    result = dt.fromtimestamp(_get_date_added(lead)).date()
    return result


def _get_house_id(lead: json) -> int:
    lead_status = _get_status(lead)
    if lead_status in (30, 50, 100):
        result = lead['sell_parent_id']
    else:
        if 'estate_buy_housesInterest' in lead['entity_attrs']:
            result = lead['entity_attrs']['estate_buy_housesInterest'][0]
        else:
            result = None
    return result


@logger.catch()
def get_leads():
    try:
        with MacroBIDB(host, username, password, database) as db:
            db.create_table('Leads', captions_Leads, True)
            date_from = dt.strftime(db.get_maximum_date('Leads') - timedelta(1), '%d.%m.%Y')
            data = get_leads_data(date_from)
            db.insert_data('Leads', captions_Leads, data, use_leads=True)
        logger.success('Leads updated.')
    except ConnectionDBError as err:
        logger.error(f'Leads. Unable to connect to DB {str(err)}')
    except SQLError as err:
        logger.error(f'Leads. Something wrong with query: {str(err)}')


if __name__ == '__main__':
    get_leads()
