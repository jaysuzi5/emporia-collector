from datetime import datetime, timezone, timedelta
import os
from dotenv import load_dotenv
from emporia.emporia import Emporia
import requests
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_URL = "http://home.dev.com/api/v1/emporia/"
API_SEARCH_URL = "http://home.dev.com/api/v1/emporia/search"
API_DELETE_URL = "http://home.dev.com/api/v1/emporia/{id}"

def _get_emporia_data(emporia, days_back):
    return emporia.get_usage(days_back=days_back)

def _load_emporia_data(usages):
    inserted_records = 0
    for usage in usages:
        # Convert 'instant' datetime to ISO 8601 string with UTC 'Z'
        instant_iso = usage['instant'].astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        name = usage.get("name", "")
        payload = {
            "instant": instant_iso,
            "scale": usage.get("scale", ""),
            "device_id": usage.get("deviceGid", 0),
            "channel_num": usage.get("channelNum", ""),
            "name": name,
            "usage": usage.get("usage", 0),
            "unit": usage.get("unit", ""),
            "percentage": usage.get("percentage", 0)
        }

        try:
            response = requests.post(API_URL, json=payload)
            if response.status_code in (200, 201):
                inserted_records += 1
            else:
                logger.error(f"emporia-collector._load_emporia_data: Failed: : {response.status_code}, "
                              f"{response.text}")
        except requests.RequestException as e:
            logger.error(f"emporia-collector._load_emporia_data: Exception: {e}")
    return inserted_records

def _get_local_data(date_string):
    payload = {
        "start_date": date_string
    }
    try:
        response = requests.post(API_SEARCH_URL, json=payload)
        if response.status_code in (200, 201):
            return response.json()
        else:
            logger.error(f"emporia-collector._get_local_data: Failed: : {response.status_code}, {response.text}")
    except requests.RequestException as e:
        logger.error(f"emporia-collector._get_local_data: Exception: {e}")


def _delete_local_data(record_id):
    url = API_DELETE_URL
    url = url.format(id=record_id)
    try:
        response = requests.delete(url)
        if response.status_code in (200, 201):
            pass
        else:
            logger.error(f"emporia-collector._delete_local_data: Failed to delete")
    except requests.RequestException as e:
        logger.error(f"emporia-collector._delete_local_data: Exception: {e}")


def _load_day(emporia, days_back=0):
    total_records = 0
    instant = datetime.now(timezone.utc) - timedelta(days=days_back)
    logger.info(f'"emporia-collector._load_day: collecting data starting at: {datetime.now()} for: {instant}')
    date_string = instant.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    usages = _get_emporia_data(emporia, days_back)
    logger.info(f'emporia-collector._load_day: {len(usages)} usages returned from Emporia')
    if usages and len(usages) > 0:
        # First remove the local data
        response = _get_local_data(date_string)
        for record in response:
            _delete_local_data(record['id'])
        # Load the local data
        records_inserted = _load_emporia_data(usages)
        total_records += records_inserted
    logger.info(f'"emporia-collector._load_day: finished: {datetime.now()} collected: {total_records} records')


def main():
    load_dotenv()
    emporia = Emporia(os.getenv("USERNAME"), os.getenv("PASSWORD"), os.getenv("CLIENT_ID"))
    _load_day(emporia, days_back=1)
    _load_day(emporia, days_back=0)


if __name__ == "__main__":
    main()
