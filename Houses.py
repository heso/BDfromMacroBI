import requests
import json
import os
import urllib.parse
from dotenv import load_dotenv
from DB_Config import captions_Houses, captions_types_translations, types_translations
from PostgreSQL import *

load_dotenv()


def get_data():

    api_key = urllib.parse.quote_plus(os.environ.get('api_key'))

    url = (f'https://api.macroserver.ru/analytics/goodbi/{api_key}/getEstateComplexes.json?')

    request = requests.get(url).content
    json_objects = json.loads(request)

    flag_running = len(json_objects['data']) != 0

    data = []

    while flag_running:
        for complex in json_objects['data']:
            complex_id = complex['id']
            complex_name = complex['name']
            for houses in complex['houses']:
                house_id = houses['id']
                house_name = houses['name']
                house_address = houses['address']
                house_status = houses['status']

                data.append((house_id,
                             house_name,
                             complex_id,
                             complex_name,
                             house_address,
                             house_status))

        if json_objects['next'] is None:
            flag_running = False
        else:
            url_new = url + str(json_objects['next'])
            request = requests.get(url_new).content
            json_objects = json.loads(request)
    return data


def main():
    host = os.environ.get('db_host')
    username = os.environ.get('db_username')
    password = os.environ.get('db_password')
    database = os.environ.get('db_base')

    data = get_data()

    try:
        with MacroBIDB(host, username, password, database) as db:
            db.create_table('Types_translations', captions_types_translations)
            db.insert_data('Types_translations', captions_types_translations, types_translations)

            db.create_table('Houses', captions_Houses)
            db.insert_data('Houses', captions_Houses, data)

    except ConnectionError as err:
        print(f'Unable to connect to DB {str(err)}')
    except SQLError as err:
        print(f'Something wrong with query: {str(err)}')
    except Exception as err:
        print(f'Something went wrong: {str(err)}')


if __name__ == '__main__':
    main()
