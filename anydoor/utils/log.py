import logging
from colorama import init, Fore, Back

init(autoreset=True)


class ColorFormatter(logging.Formatter):
    # Change this dictionary to suit your coloring needs!
    COLORS = {
        "WARNING": Fore.RED,
        "ERROR": Fore.RED + Back.WHITE,
        "DEBUG": Fore.BLUE,
        "INFO": Fore.GREEN,
        "CRITICAL": Fore.RED + Back.WHITE,
    }

    def format(self, record):
        color = self.COLORS.get(record.levelname, "")
        if color:
            record.name = color + record.name
            record.levelname = color + record.levelname
            record.msg = color + record.msg
        return logging.Formatter.format(self, record)


class ColorLogger(logging.Logger):
    def __init__(self, name):
        logging.Logger.__init__(self, name, logging.DEBUG)
        color_formatter = ColorFormatter("%(asctime)-10s %(levelname)-10s %(message)s")
        console = logging.StreamHandler()
        console.setFormatter(color_formatter)
        self.addHandler(console)


# logging.setLoggerClass(ColorLogger)
logger = logging.getLogger("anydoor")
if not logger.hasHandlers():
    color_formatter = ColorFormatter("%(asctime)-10s %(levelname)-10s %(message)s")
    console = logging.StreamHandler()
    console.setFormatter(color_formatter)
    console.setLevel(logging.INFO)
    logger.addHandler(console)
    logger.setLevel(logging.INFO)
