__all__ = ['get_houses']

from loguru import logger

from .usefull_functions import get_json_from_url
from ..postgreSQL import MacroBIDB, ConnectionDBError, SQLError
from ..configs import captions_types_translations, types_translations, \
                      captions_Houses, \
                      url_houses, \
                      host, database, \
                      username, password


def get_houses_data():

    url = url_houses
    json_objects = get_json_from_url(url)

    flag_running = len(json_objects['data']) != 0
    data = []

    while flag_running:
        for complex_object in json_objects['data']:
            complex_id = complex_object['id']
            complex_name = complex_object['name']
            for houses in complex_object['houses']:
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
            json_objects = get_json_from_url(url_new)
    return data


@logger.catch()
def get_houses():
    data = get_houses_data()
    try:
        with MacroBIDB(host, username, password, database) as db:
            db.create_table('Types_translations', captions_types_translations)
            db.insert_data('Types_translations', captions_types_translations, types_translations)
            db.create_table('Houses', captions_Houses)
            db.insert_data('Houses', captions_Houses, data)
        logger.success('Houses updated.')
    except ConnectionDBError as err:
        logger.error(f'Houses. Unable to connect to DB {str(err)}')
    except SQLError as err:
        logger.error(f'Houses. Something wrong with query: {str(err)}')


if __name__ == '__main__':
    get_houses()
