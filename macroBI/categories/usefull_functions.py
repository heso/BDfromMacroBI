import requests
import json
from datetime import datetime

from ..db_config import path_requests_text


def get_json_from_url(url: str):
    request = requests.get(url).content
    try:
        json_objects = json.loads(request)
        return json_objects
    except json.JSONDecodeError:
        now = datetime.now()
        with open(f'{path_requests_text}\{now.strftime("%Y%m%d_%H%M%S")}.txt', 'wb') as f:
            f.write(request)


if __name__ == '__main__':
    pass
