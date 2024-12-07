import argparse
import logging

from millegrilles_messages.bus.BusConfiguration import MilleGrillesBusConfiguration

LOGGING_NAMES = [__name__, 'millegrilles_messages', 'millegrilles_ceduleur']


def __adjust_logging(args: argparse.Namespace):
    logging.basicConfig()
    if args.verbose is True:
        for log in LOGGING_NAMES:
            logging.getLogger(log).setLevel(logging.DEBUG)
    else:
        for log in LOGGING_NAMES:
            logging.getLogger(log).setLevel(logging.INFO)


def _parse_command_line():
    parser = argparse.ArgumentParser(description="Ticker for MilleGrilles that emits a tick signal every minute")
    parser.add_argument(
        '--verbose', action="store_true", required=False,
        help="More logging"
    )

    args = parser.parse_args()
    __adjust_logging(args)
    return args


class TickerConfiguration(MilleGrillesBusConfiguration):

    def __init__(self):
        super().__init__()

    @staticmethod
    def load():
        # Override
        config = TickerConfiguration()
        _parse_command_line()
        config.parse_config()
        return config
