import inspect
import logging
import os
import uuid
from datetime import datetime, timezone
from enum import Enum
from dataclasses import dataclass
from dotenv import load_dotenv
from typing import Optional


class EventType(Enum):
    TRANSACTION_START = "transactionStart"
    TRANSACTION_END = "transactionEnd"
    SUB_TRANSACTION_START = "subTransactionStart"
    SUB_TRANSACTION_END = "subTransactionEnd"

@dataclass
class LoggingInfo:
    component: str
    component_type: str


class Logger:
    """
    Logger class for structured transaction and sub-transaction logging.

    Usage Example:
        from logger_module import Logger, LoggingInfo, EventType

        # Create a LoggingInfo object (could be loaded from YAML)
        logging_info = LoggingInfo(
            component="emporia-collector",
            component_type="python"
        )

        # Initialize the Logger
        logger = Logger(logging_info)

        # --- Transaction Start ---
        txn_start = logger.transaction_event(
            event_type=EventType.TRANSACTION_START,
            payload={"amount": 100, "currency": "USD"}
        )

        # --- Sub-Transaction Start ---
        sub_txn_start = logger.transaction_event(
            event_type=EventType.SUB_TRANSACTION_START,
            transaction=txn_start,
            source_component="payment-processor",
            payload={"step": "authorize_card"}
        )

        # --- Log an Info Message ---
        logger.message(
            transaction=sub_txn_start,
            message="Card authorization approved",
            data={"approval_code": "XYZ123"}
        )

        # --- Sub-Transaction End ---
        logger.transaction_event(
            event_type=EventType.SUB_TRANSACTION_END,
            transaction=sub_txn_start,
            return_code=200
        )

        # --- Transaction End ---
        logger.transaction_event(
            event_type=EventType.TRANSACTION_END,
            transaction=txn_start,
            return_code=200
        )
    """

    def __init__(self, logging_info: LoggingInfo, log_level: Optional[str] = None):
        """
        Initialize a structured logger.

        Args:
            logging_info (LoggingInfo): Contains component metadata.
            log_level (str, optional): Override default log level or env `LOG_LEVEL`.
        """
        load_dotenv()
        self._logging_info = logging_info
        self._log_level_str = log_level or os.getenv("LOG_LEVEL", "INFO").upper()
        self._logger = logging.getLogger(self._logging_info.component)
        log_level = getattr(logging, self._log_level_str, logging.INFO)
        logging.basicConfig(level=log_level, format='%(message)s')

    def transaction_event(self,
                          event_type: EventType,
                          payload: dict = None,
                          transaction: dict = None,
                          source_component: str = None,
                          return_code: int = None) -> dict:
        """
        Log a transaction or sub-transaction event with structured metadata.

        Args:
            event_type (EventType): Type of event (START/END for transaction or sub-transaction).
            payload (dict, optional): Custom payload for this event.
            transaction (dict, optional): Transaction/sub-transaction dictionary returned from
                                          a prior START event (required for END events).
            source_component (str, optional): Component name for sub-transactions (required for SUB_TRANSACTION_START).
            return_code (int, optional): Return code, typically 200 for success, 500 for failure

        Returns:
            dict: The structured log message (includes transaction_id, timestamps, etc.).

        Raises:
            ValueError: If required parameters are missing for the event type.
        """
        log_message = {}
        data = validate_transaction_event(event_type, transaction, source_component, return_code)
        log_message["level"] = self._log_level_str
        log_message["event_type"] = event_type.value
        log_message["timestamp"] = data["timestamp"]
        log_message["transaction_id"] = data["transaction_id"]
        log_message["component"] = self._logging_info.component
        log_message["component_type"] = self._logging_info.component_type
        if payload: log_message["payload"] = payload
        if "return_code" in data:
            log_message["return_code"] = data["return_code"]
        if "duration" in data:
            log_message["duration"] = data["duration"]
        if "source_transaction_id" in data:
            log_message["source_transaction_id"] = data["source_transaction_id"]
        if "source_component" in data:
            log_message["source_component"] = data["source_component"]
        self._logger.info(log_message)
        return log_message

    def message(self,
                transaction: dict,
                message: str,
                exception: Exception = None,
                stack_trace: str = None,
                data: dict = None,
                error: bool = False,
                debug: bool = False) -> None:
        """
        Log a structured message tied to a transaction.

        Args:
            transaction (dict): Transaction/sub-transaction dictionary (must include `transaction_id`).
            message (str): The message text to log.
            exception (Exception, optional): Exception object to include in the log.
            stack_trace (str, optional): Stack trace string for debugging.
            data (dict, optional): Additional structured data to include.
            error (bool, optional): If True, log as ERROR level.
            debug (bool, optional): If True, log as DEBUG level (overrides INFO).

        Notes:
            - Defaults to INFO level unless `error=True` or `debug=True`.
            - Automatically captures caller class and method name if available.
        """
        log_message = {}
        message_data = validate_message(transaction)
        log_message["event_type"] = "message"
        log_message["message"] = message
        log_message["timestamp"] = message_data["timestamp"]
        log_message["transaction_id"] = message_data["transaction_id"]
        log_message["component"] = self._logging_info.component
        log_message["component_type"] = self._logging_info.component_type
        if "source_component" in message_data:
            log_message["source_component"] = message_data["source_component"]
        if "source_transaction_id" in message_data:
            log_message["source_component"] = message_data["source_transaction_id"]
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


def validate_transaction_event(event_type: EventType,
                               transaction: dict = None,
                               source_component: str = None,
                               return_code: int = None) -> dict:
    data = {}
    timestamp = datetime.now(timezone.utc).isoformat()
    data['timestamp'] = timestamp
    if event_type == EventType.TRANSACTION_START:
        data['transaction_id'] = str(uuid.uuid4())
    if event_type == EventType.TRANSACTION_END:
        if not transaction:
            raise ValueError("transaction MUST be passed in for a TRANSACTION_END")
        if 'transaction_id' not in transaction:
            raise ValueError("transaction_id MUST be in transaction for a TRANSACTION_END")
        if 'timestamp' not in transaction:
            raise ValueError("timestamp MUST be in transaction for a TRANSACTION_END")
        if not return_code:
            raise ValueError("return_code MUST be passed for a TRANSACTION_END")
        data['transaction_id'] = transaction['transaction_id']
    elif event_type == EventType.SUB_TRANSACTION_START:
        if not transaction:
            raise ValueError("transaction MUST be passed in for a SUB_TRANSACTION_START")
        if not source_component:
            raise ValueError("source_component MUST be passed in for a SUB_TRANSACTION_START")
        if 'transaction_id' not in transaction:
            raise ValueError("transaction_id MUST be in transaction for a SUB_TRANSACTION_START")
        data["source_component"] = source_component
        data['transaction_id'] = transaction['transaction_id']
        data['source_transaction_id'] = str(uuid.uuid4())
    elif event_type == EventType.SUB_TRANSACTION_END:
        if not transaction:
            raise ValueError("transaction MUST be passed in for a SUB_TRANSACTION_END")
        if 'transaction_id' not in transaction:
            raise ValueError("transaction_id MUST be in transaction for a SUB_TRANSACTION_END")
        if 'source_component' not in transaction:
            raise ValueError("source_component MUST be in transaction for a SUB_TRANSACTION_END")
        if 'timestamp' not in transaction:
            raise ValueError("timestamp MUST be in transaction for a SUB_TRANSACTION_END")
        if not return_code:
            raise ValueError("return_code MUST be passed for a SUB_TRANSACTION_END")
        data["source_component"] = transaction['source_component']
        data['transaction_id'] = transaction['transaction_id']
        data['source_transaction_id'] = transaction['source_transaction_id']
    if event_type == EventType.TRANSACTION_END or event_type == EventType.SUB_TRANSACTION_END:
        transaction_start = transaction["timestamp"]
        time_difference = datetime.fromisoformat(timestamp) - datetime.fromisoformat(transaction_start)
        data['duration'] = time_difference.total_seconds()
        data['return_code'] = return_code
    return data

def validate_message(transaction:dict) -> dict:
    data = {}
    timestamp = datetime.now(timezone.utc).isoformat()
    data['timestamp'] = timestamp
    if not transaction:
        raise ValueError("transaction MUST be passed in for a MESSAGE")
    if 'transaction_id' not in transaction:
        raise ValueError("transaction_id MUST in transaction for a MESSAGE")
    data["transaction_id"] = transaction["transaction_id"]
    if "source_component" in transaction:
        data["source_component"] = transaction['source_component']
    if "source_transaction_id" in transaction:
        data['source_transaction_id'] = transaction['source_transaction_id']
    return data
