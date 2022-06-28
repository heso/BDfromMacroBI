import os

from loguru import logger
from notifiers.logging import NotificationHandler
from dotenv import load_dotenv

import macroBI
load_dotenv()


@logger.catch()
def main():
    defaults = {
        'token': os.environ.get('telegram_token'),
        'chat_id': os.environ.get('chat_id')
    }
    handler = NotificationHandler('telegram', defaults=defaults)

    logger.add('logs/log.json',
               format='{time:YYYY-MM-DD HH:mm:ss} {level} {message}',
               level='INFO',
               rotation='1 MB',
               retention='30 days',
               serialize=False)
    logger.add(handler, level="ERROR")

    macroBI.get_houses()
    macroBI.get_leads()
    macroBI.get_deals()


if __name__ == '__main__':
    main()
