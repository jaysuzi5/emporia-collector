from datetime import datetime, timezone
import inspect
import logging
import os
import uuid
from dotenv import load_dotenv


class JsonLogger:
    def __init__(self):
        load_dotenv()
        self._config = None
        self._transaction_id = None
        self._logger = None
        self._request = {}
        self._log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()

    def define(self, config: dict) -> None:
        validate_configuration(config)
        self._config = config
        self._transaction_id = str(uuid.uuid4())
        self._logger = logging.getLogger(self._config['logging_info']['component'])
        log_level = getattr(logging, self._log_level_str, logging.INFO)
        logging.basicConfig(level=log_level)

    def request(self, payload: dict = None) -> None:
        log_message = {
            "level": "INFO",
            "event": "Request",
            "component": self._config['logging_info']['component'],
            "component_type": self._config['logging_info']['component_type'],
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "transaction_id": self._transaction_id,
        }
        if payload:
            log_message["payload"] = payload
        self._request = log_message
        self._logger.info(log_message)

    def response(self, return_code: int = 200, payload: dict = None) -> None:
        request_time = self._request["timestamp"]
        response_time = datetime.now(timezone.utc).isoformat()
        time_difference = datetime.fromisoformat(response_time) - datetime.fromisoformat(request_time)
        duration = time_difference.total_seconds()
        log_message = {
            "level": "INFO",
            "event": "Response",
            "component": self._config['logging_info']['component'],
            "component_type": self._config['logging_info']['component_type'],
            "timestamp": response_time,
            "transaction_id": self._transaction_id,
            "duration": duration,
            "return_code": return_code
        }
        if payload:
            log_message["payload"] = payload
        self._logger.info(log_message)

    def source_request(self, source_component: str, payload: dict = None) -> dict:
        log_message = {
            "level": "INFO",
            "event": "SourceRequest",
            "component": self._config['logging_info']['component'],
            "component_type": self._config['logging_info']['component_type'],
            "source_component": source_component,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "transaction_id": self._transaction_id,
            "source_transaction_id": str(uuid.uuid4())
        }
        if payload:
            log_message["payload"] = payload
        self._logger.info(log_message)
        return log_message

    def source_response(self, source_request: dict, return_code: int = 200, payload: dict = None) -> None:
        source_request_time = source_request["timestamp"]
        source_response_time = datetime.now(timezone.utc).isoformat()
        time_difference = datetime.fromisoformat(source_response_time) - datetime.fromisoformat(source_request_time)
        duration = time_difference.total_seconds()
        log_message = {
            "level": "INFO",
            "event": "SourceResponse",
            "component": self._config['logging_info']['component'],
            "component_type": self._config['logging_info']['component_type'],
            "source_component": source_request["source_component"],
            "timestamp": source_response_time,
            "transaction_id": self._transaction_id,
            "source_transaction_id": source_request["source_transaction_id"],
            "duration": duration,
            "return_code": return_code
        }
        if payload:
            log_message["payload"] = payload
        self._logger.info(log_message)


    def message(self, message:str, exception: Exception = None, stack_trace: str = None, data: dict = None,
                source_request: dict = None, error: bool = False, debug: bool = False) -> None:
        log_message = {
            "message": message,
            "event": "Message",
            "component": self._config['logging_info']['component'],
            "component_type": self._config['logging_info']['component_type'],
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "transaction_id": self._transaction_id
        }
        if source_request:
            log_message["source_component"] = source_request["source_component"]
            log_message["source_transaction_id"] = source_request["source_transaction_id"]
        if data:
            log_message["data"] = data
        else:
            log_message["data"] = {}
        if exception:
            log_message["data"]["exception"] = str(exception)
        if stack_trace:
            log_message["stack_trace"] = stack_trace

        # Log class and function if the class exist, otherwise just the method
        try:
            caller_frame = inspect.stack()[1].frame
            self_obj = caller_frame.f_locals.get('self', None)
            if self_obj:
                log_message["method"] = type(self_obj).__name__ + caller_frame.f_code.co_name
            else:
                log_message["method"] = caller_frame.f_code.co_name
        except: # Don't want the logging to throw an exception
            pass

        # Do the actual logging
        if exception or error:
            log_message["level"] = "ERROR"
            self._logger.error(log_message)
        elif debug:
            log_message["level"] = "DEBUG"
            self._logger.debug(log_message)
        else:
            log_message["level"] = "INFO"
            self._logger.info(log_message)


def validate_configuration(config:dict) -> None:
    if config is None:
        raise ValueError("Configuration is missing or could not be loaded.")
    if not isinstance(config, dict):
        raise TypeError("Configuration must be a dictionary.")
    if "logging_info" not in config:
        raise ValueError("Configuration is missing logging_info.")
    if "level" not in config['logging_info']:
        raise ValueError("Configuration is missing level within logging_info.")
    if "component" not in config['logging_info']:
        raise ValueError("Configuration is missing component within logging_info.")
    if "component_type" not in config['logging_info']:
        raise ValueError("Configuration is missing component_type within logging_info.")
