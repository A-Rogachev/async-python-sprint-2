import logging


class Color:
    """
    Цвета для различных видов сообщений
    """
    GREEN: str = '\033[92m'
    RED: str = '\033[91m'
    YELLOW: str = '\033[93m'
    RESET: str = '\033[0m'


class ColoredFormatter(logging.Formatter):
    def format(self, record):
        """
        Форматирование сообщение различными цветами.
        """
        if record.levelno == logging.INFO:
            record.msg = f"{Color.GREEN}{record.msg}{Color.RESET}"
        elif record.levelno == logging.WARNING:
            record.msg = f"{Color.YELLOW}{record.msg}{Color.RESET}"
        elif record.levelno == logging.ERROR:
            record.msg = f"{Color.RED}{record.msg}{Color.RESET}"
        return super().format(record)


def setup_logger(name='root', level=logging.INFO) -> logging.RootLogger:
    """
    Настройка логгера.
    """

    handler = logging.StreamHandler()
    handler.setFormatter(
        ColoredFormatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        ),
    )

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger
