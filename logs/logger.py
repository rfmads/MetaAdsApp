# logs/logger.py
import logging
import sys

LOGGER_NAME = "metaads"

def get_logger() -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False  # مهم جدًا: يمنع تكرار logs عبر root logger

    # ✅ امسح أي handlers قديمة (حتى لو اتعمل import أكثر من مرة)
    if logger.handlers:
        logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(name)s - %(message)s")
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    return logger

logger = get_logger()
