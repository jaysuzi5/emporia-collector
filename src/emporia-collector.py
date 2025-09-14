import requests
import yaml
import os
import traceback
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from emporia.emporia import Emporia
from framework.json_logger import JsonLogger

# Define Globally.  This will be assigned after configuration is read
json_logger = JsonLogger()
API_URL = ""

def _load_config(path: str = "src/configuration/config.yaml") -> dict:
    with open(path, "r") as f:
        config = yaml.safe_load(f)  # safe_load ensures security
    return config


def _get_emporia_data(emporia, days_back):
    usages = []
    return_code = 200
    payload = {"days_back": days_back}
    source_request = json_logger.source_request(source_component="Emporia", payload=payload)
    try:
        usages = emporia.get_usage(days_back=days_back)
        if usages:
            payload["usage_records"] = len(usages)
    except Exception as ex:
        return_code = 500
        payload["usage_records"] = 0
        stack_trace = traceback.format_exc()
        message = "Exception collecting Emporia data"
        data = {
            "days_back": days_back
        }
        json_logger.message(message=message, exception=ex, stack_trace=stack_trace, source_request=source_request, data=data)

    json_logger.source_response(source_request=source_request, return_code=return_code, payload=payload)
    return return_code, usages

def _get_local_data(instant: datetime) -> tuple:
    return_code = 200
    payload = {}
    results = None
    date_string = instant.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    payload["start_date"] = date_string
    source_request = json_logger.source_request(source_component="emporia: Local Search", payload=payload)

    try:
        response = requests.post(API_URL+"search", json=payload)
        payload['status_code'] = str(response.status_code)
        if response.status_code == 200:
            results = response.json()
            if results:
                payload['result_records'] = len(results)
        else:
            return_code = 500
            payload['result_records'] = 0
            message = "Exception collecting local data: Non-200 status code"
            data = {
                "instant": instant
            }
            json_logger.message(message=message, source_request=source_request, data=data)
    except requests.RequestException as ex:
        return_code = 500
        payload['result_records'] = 0
        stack_trace = traceback.format_exc()
        message = "Exception collecting local data"
        data = {
            "instant": instant
        }
        json_logger.message(message=message, exception=ex, stack_trace=stack_trace,
                            source_request=source_request, data=data)

    json_logger.source_response(source_request=source_request, return_code=return_code, payload=payload)
    return return_code, results

def _delete_local_data(response) -> tuple:
    payload = {}
    return_code = 200
    total_records = len(response)
    deleted = 0
    errors = 0
    payload["total_records"] = total_records
    source_request = json_logger.source_request(source_component="emporia: Local Delete", payload=payload)
    try:
        for record in response:
            record_id = str(record['id'])
            response = requests.delete(API_URL + record_id)
            if response.status_code == 200:
                deleted += 1
            else:
                return_code = 500
                errors += 1
                message = "Exception deleting local data: Non-200 status code"
                data = {
                    "record_id": record_id
                }
                json_logger.message(message=message, source_request=source_request, data=data)
    except requests.RequestException as ex:
        return_code = 500
        errors = total_records - deleted
        stack_trace = traceback.format_exc()
        message = "Exception deleting local data"
        json_logger.message(message=message, exception=ex, stack_trace=stack_trace, source_request=source_request)
    payload['deleted'] = deleted
    payload['errors'] = errors
    json_logger.source_response(source_request=source_request, return_code=return_code, payload=payload)
    return return_code, deleted, errors

def _load_emporia_data(usages):
    payload = {}
    return_code = 200
    total_records = len(usages)
    inserted = 0
    errors = 0
    payload["total_records"] = total_records
    source_request = json_logger.source_request(source_component="emporia: Local Insert", payload=payload)
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
            if response.status_code == 200:
                inserted += 1
            else:
                return_code = 500
                errors += 1
                message = "Exception inserting local data: Non-200 status code"
                data = {
                    "name": name
                }
                json_logger.message(message=message, source_request=source_request, data=data)
        except requests.RequestException as ex:
            return_code = 500
            errors = total_records - inserted
            stack_trace = traceback.format_exc()
            message = "Exception deleting local data"
            json_logger.message(message=message, exception=ex, stack_trace=stack_trace, source_request=source_request)
    payload['inserted'] = inserted
    payload['errors'] = errors
    json_logger.source_response(source_request=source_request, return_code=return_code, payload=payload)
    return return_code, inserted, errors


def _load_day(emporia, days_back=0) -> tuple:
    json_logger.message(f"staring _load_day with days_back: {days_back}", debug=True)
    instant = datetime.now(timezone.utc) - timedelta(days=days_back)
    total_records = 0
    total_errors = 0
    deleted = 0
    return_code, usages = _get_emporia_data(emporia, days_back)
    if return_code == 200 and usages and len(usages) > 0:
        # First remove the local data
        return_code, response = _get_local_data(instant)
        if return_code == 200:
            return_code, deleted, errors = _delete_local_data(response)
            total_errors += errors
            if return_code == 200:
                # Load the local data
                return_code, records_inserted, errors = _load_emporia_data(usages)
                total_records += records_inserted
                total_errors += errors

    return return_code, instant, total_records, deleted, total_errors


def main():
    global json_logger, API_URL
    payload= {}
    response_return_code = 200
    total_records = 0
    total_deleted = 0
    total_errors = 0
    load_dotenv()
    API_URL = os.getenv("API_URL")
    config = _load_config()
    json_logger.define(config)
    json_logger.request()
    # Create the Emporia object that will be used to call the external Emporia APIs
    emporia = Emporia(os.getenv("USERNAME"), os.getenv("PASSWORD"), os.getenv("CLIENT_ID"))

    # Call for yesterday's data and load it to PostgreSQL
    return_code, instant, records, deleted, errors = _load_day(emporia, days_back=1)
    if return_code > response_return_code:
        response_return_code = return_code
    total_records += records
    total_deleted += deleted
    total_errors += errors
    payload['details'] = []
    payload["details"].append(
        {
            'instant': instant.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            'records': records,
            'deleted': deleted,
            'errors': errors
        })

    # Call for today's data and load it to PostgreSQL
    return_code, instant, total_records, deleted, errors = _load_day(emporia, days_back=0)
    if return_code > response_return_code:
        response_return_code = return_code
    total_records += records
    total_deleted += deleted
    total_errors += errors
    payload["details"].append(
        {
            'instant': instant.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            'records': records,
            'deleted': deleted,
            'errors': errors
        })

    payload['total_records'] = total_records
    payload['total_deleted'] = total_deleted
    payload['total_errors'] = total_errors

    json_logger.response(payload=payload, return_code=response_return_code)

if __name__ == "__main__":
    main()
