import logging
from rich.logging import RichHandler

logging.basicConfig(
    level="ERROR",
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler()],
)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
