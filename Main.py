import os

from loguru import logger
from notifiers.logging import NotificationHandler
from dotenv import load_dotenv

import Houses
import Deals
import Leads

load_dotenv()


@logger.catch()
def main():
    defaults = {
        'token': os.environ.get('telegram_token'),
        'chat_id':  os.environ.get('chat_id')
    }
    handler = NotificationHandler('telegram', defaults=defaults)

    logger.add('logs/log.json',
               format='{time:YYYY-MM-DD HH:mm:ss} {level} {message}',
               level='INFO',
               rotation='10 MB',
               retention='30 days',
               serialize=True)
    logger.add(handler, level="ERROR")

    Houses.main()
    Deals.main()
    Leads.main()


if __name__ == '__main__':
    main()
