from loguru import logger

import Houses
import Deals
import Leads


@logger.catch()
def main():
    logger.add('logs/log.json',
               format='{time:YYYY-MM-DD HH:mm:ss} {level} {message}',
               level='INFO',
               rotation='10 MB',
               retention='30 days',
               serialize=True)
    Houses.main()
    Deals.main()
    Leads.main()


if __name__ == '__main__':
    main()
