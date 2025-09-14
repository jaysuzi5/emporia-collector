import requests

import os
import traceback
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from emporia.emporia import Emporia
from framework.jLogger import LoggingInfo, Logger, EventType
from framework.jConfig import Config


class EmporiaCollector:

    def __init__(self, config):
        load_dotenv()
        self._config = config
        logging_info = LoggingInfo(**self._config.get("logging_info", {}))
        self._logger = Logger(logging_info)
        self._api_url = os.getenv("API_URL")
        self._transaction = None

    def process(self):
        payload= {}
        response_return_code = 200
        total_records = 0
        total_deleted = 0
        total_errors = 0

        self._transaction = self._logger.transaction_event(EventType.TRANSACTION_START)
        # Create the Emporia object that will be used to call the external Emporia APIs
        emporia = Emporia(os.getenv("USERNAME"), os.getenv("PASSWORD"), os.getenv("CLIENT_ID"))

        # Call for yesterday's data and load it to Postgres
        return_code, instant, records, deleted, errors = self._load_day(emporia, days_back=1)
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

        # Call for today's data and load it to Postgres
        return_code, instant, total_records, deleted, errors = self._load_day(emporia, days_back=0)
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
        self._logger.transaction_event(EventType.TRANSACTION_END, transaction=self._transaction,
                                       payload=payload, return_code=response_return_code)

    def _load_day(self, emporia, days_back=0) -> tuple:
        self._logger.message(self._transaction, message=f"staring _load_day with days_back: {days_back}", debug=True)
        instant = datetime.now(timezone.utc) - timedelta(days=days_back)
        total_records = 0
        total_errors = 0
        deleted = 0
        return_code, usages = self._get_emporia_data(emporia, days_back)
        if return_code == 200 and usages and len(usages) > 0:
            # First remove the local data
            return_code, response = self._get_local_data(instant)
            if return_code == 200:
                return_code, deleted, errors = self._delete_local_data(response)
                total_errors += errors
                if return_code == 200:
                    # Load the local data
                    return_code, records_inserted, errors = self._load_emporia_data(usages)
                    total_records += records_inserted
                    total_errors += errors
        return return_code, instant, total_records, deleted, total_errors

    def _get_emporia_data(self, emporia, days_back):
        usages = []
        return_code = 200
        payload = {"days_back": days_back}
        source_transaction = self._logger.transaction_event(EventType.SUB_TRANSACTION_START, payload=payload,
                                                        source_component="Emporia", transaction=self._transaction)
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
            self._logger.message(message=message, exception=ex, stack_trace=stack_trace, data=data,
                                 transaction=source_transaction)
        self._logger.transaction_event(EventType.SUB_TRANSACTION_END, transaction=source_transaction,
                                       payload=payload, return_code=return_code)
        return return_code, usages

    def _get_local_data(self, instant: datetime) -> tuple:
        return_code = 200
        payload = {}
        results = None
        date_string = instant.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        payload["start_date"] = date_string
        source_transaction = self._logger.transaction_event(EventType.SUB_TRANSACTION_START, payload=payload,
                                                            source_component="emporia: Local Search",
                                                            transaction=self._transaction)
        try:
            response = requests.post(self._api_url+"search", json=payload)
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
                self._logger.message(message=message, data=data, transaction=source_transaction)
        except requests.RequestException as ex:
            return_code = 500
            payload['result_records'] = 0
            stack_trace = traceback.format_exc()
            message = "Exception collecting local data"
            data = {
                "instant": instant
            }
            self._logger.message(message=message, exception=ex, stack_trace=stack_trace, data=data,
                                 transaction=source_transaction)
        self._logger.transaction_event(EventType.SUB_TRANSACTION_END, transaction=source_transaction,
                                       payload=payload, return_code=return_code)
        return return_code, results

    def _delete_local_data(self, response) -> tuple:
        payload = {}
        return_code = 200
        total_records = len(response)
        deleted = 0
        errors = 0
        payload["total_records"] = total_records
        source_transaction = self._logger.transaction_event(EventType.SUB_TRANSACTION_START, payload=payload,
                                                            source_component="emporia: Local Delete",
                                                            transaction=self._transaction)

        for record in response:
            try:
                record_id = str(record['id'])
                response = requests.delete(self._api_url + record_id)
                if response.status_code == 200:
                    deleted += 1
                else:
                    return_code = 500
                    errors += 1
                    message = "Exception deleting local data: Non-200 status code"
                    data = {
                        "record_id": record_id
                    }
                    self._logger.message(message=message, data=data, transaction=source_transaction)
            except requests.RequestException as ex:
                return_code = 500
                errors = total_records - deleted
                stack_trace = traceback.format_exc()
                message = "Exception deleting local data"
                self._logger.message(message=message, exception=ex, stack_trace=stack_trace,
                                     transaction=source_transaction)
        payload['deleted'] = deleted
        payload['errors'] = errors
        self._logger.transaction_event(EventType.SUB_TRANSACTION_END, transaction=source_transaction,
                                       payload=payload, return_code=return_code)

        return return_code, deleted, errors

    def _load_emporia_data(self, usages):
        payload = {}
        return_code = 200
        total_records = len(usages)
        inserted = 0
        errors = 0
        payload["total_records"] = total_records
        source_transaction = self._logger.transaction_event(EventType.SUB_TRANSACTION_START, payload=payload,
                                                            source_component="emporia: Local Insert",
                                                            transaction=self._transaction)

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
                response = requests.post(self._api_url, json=payload)
                if response.status_code == 200:
                    inserted += 1
                else:
                    return_code = 500
                    errors += 1
                    message = "Exception inserting local data: Non-200 status code"
                    data = {
                        "name": name
                    }
                    self._logger.message(message=message, data=data, transaction=source_transaction)
            except requests.RequestException as ex:
                return_code = 500
                errors = total_records - inserted
                stack_trace = traceback.format_exc()
                message = "Exception deleting local data"
                self._logger.message(message=message, exception=ex, stack_trace=stack_trace,
                                     transaction=source_transaction)

        payload['inserted'] = inserted
        payload['errors'] = errors
        self._logger.transaction_event(EventType.SUB_TRANSACTION_END, transaction=source_transaction,
                                       payload=payload, return_code=return_code)
        return return_code, inserted, errors


def main():
    config = Config()
    collector = EmporiaCollector(config)
    collector.process()

if __name__ == "__main__":
    main()
