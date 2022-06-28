import urllib.parse
import os
from dotenv import load_dotenv

load_dotenv()

path_requests_text = 'requests/'

api_url = 'https://api.macroserver.ru/analytics/goodbi/'
api_key = urllib.parse.quote_plus(os.environ.get('api_key'))

url_leads = f'{api_url}{api_key}/getEstateBuys.json'
url_deals = f'{api_url}{api_key}/getEstateDeals.json'
url_houses = f'{api_url}{api_key}/getEstateComplexes.json?'

host = os.environ.get('db_host')
username = os.environ.get('db_username')
password = os.environ.get('db_password')
database = os.environ.get('db_base')

captions_Deals = ['id INTEGER',
                  'agreement_date DATE',
                  'date_modified DATE',
                  'status_modified_date DATE',
                  'area NUMERIC',
                  'category TEXT',
                  'status INTEGER',
                  'is_payed_reserve INTEGER',
                  'summ NUMERIC',
                  'bank BOOLEAN',
                  'bank_name TEXT',
                  'deal_program TEXT',
                  'agent TEXT',
                  'mediator_comission NUMERIC',
                  'id_house INTEGER',
                  'type_rus TEXT',
                  'complex_name TEXT',
                  'house_name TEXT']

captions_Leads = ['id INTEGER',
                  'date_added DATE',
                  'category TEXT',
                  'status TEXT',
                  'id_house INTEGER',
                  'type_rus TEXT',
                  'complex_name TEXT',
                  'house_name TEXT']

captions_Houses = ['houseID INTEGER',
                   'houseName TEXT',
                   'complexID INTEGER',
                   'complexName TEXT',
                   'houseAddress TEXT',
                   'houseStatus INTEGER']

captions_types_translations = ['type_eng TEXT',
                               'type_rus TEXT']

types_translations = [('flat', 'квартира'),
                      ('comm', 'коммерция'),
                      ('garage', 'парковка'),
                      ('land', 'земля'),
                      ('storageroom', 'кладовка'),
                      ('house', 'дом')]