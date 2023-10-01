import logging


SUCCESS = 25
logging.addLevelName(SUCCESS, 'SUCCESS')


def success(self, message, *args, **kws):
    """
    Вариант для логгера в случае успешного завершения операции.
    """
    self._log(SUCCESS, message, args, **kws)


logging.Logger.success = success


class Color:
    """
    Цвета для различных видов сообщений
    """
    GREEN: str = '\033[92m'
    RED: str = '\033[91m'
    YELLOW: str = '\033[93m'
    RESET: str = '\033[0m'
    LIGHT_CYAN: str = '\033[96m'


class ColoredFormatter(logging.Formatter):
    def format(self, record):
        """
        Форматирование сообщение различными цветами.
        """
        match record.levelno:
            case logging.INFO:
                record.msg = f"{Color.LIGHT_CYAN}{record.msg}{Color.RESET}"
            case logging.WARNING:
                record.msg = f"{Color.YELLOW}{record.msg}{Color.RESET}"
            case logging.ERROR:
                record.msg = f"{Color.RED}{record.msg}{Color.RESET}"
            case _:
                record.msg = f"{Color.GREEN}{record.msg}{Color.RESET}"
        return super().format(record)


def setup_logger(name='root', level=logging.INFO) -> logging.Logger:
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
