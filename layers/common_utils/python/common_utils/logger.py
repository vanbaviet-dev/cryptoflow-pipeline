import json
import logging
import os

_logger = logging.getLogger()
_logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

def _log(level, message, **kwargs):
    payload = {
        "level": level,
        "message": message,
        **kwargs
    }
    _logger.log(level, json.dumps(payload))

def info(message, **kwargs):
    _log(logging.INFO, message, **kwargs)

def warning(message, **kwargs):
    _log(logging.WARNING, message, **kwargs)

def error(message, **kwargs):
    _log(logging.ERROR, message, **kwargs)
