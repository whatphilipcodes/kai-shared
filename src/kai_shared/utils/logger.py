import logging
from logging import Logger

from src.kai_shared.config_shared import settings


def get_logger(name: str) -> Logger:
    logger = logging.getLogger(name)
    return logger


def setup_logging() -> None:
    logging.basicconfig_shared(
        level=settings.system.log_level.value,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
