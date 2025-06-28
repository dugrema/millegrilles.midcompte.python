import argparse
import logging

from os import environ

from millegrilles_messages.bus.BusConfiguration import MilleGrillesBusConfiguration
from millegrilles_messages.messages import Constantes as ConstantesMessages

LOGGING_NAMES = [__name__, 'millegrilles_messages', 'millegrilles_media']


def __adjust_logging(args: argparse.Namespace):
    logging.basicConfig()
    if args.verbose is True:
        for log in LOGGING_NAMES:
            logging.getLogger(log).setLevel(logging.DEBUG)
    else:
        for log in LOGGING_NAMES:
            logging.getLogger(log).setLevel(logging.INFO)


def _parse_command_line():
    parser = argparse.ArgumentParser(description="Media converter for MilleGrilles")
    parser.add_argument(
        '--verbose', action="store_true", required=False,
        help="More logging"
    )
    # parser.add_argument(
    #     '--novideo', action="store_true", required=False,
    #     help="Desactive le traitement video"
    # )
    # parser.add_argument(
    #     '--noimage', action="store_true", required=False,
    #     help="Desactive le traitement image"
    # )
    # parser.add_argument(
    #     '--fallback', action="store_true", required=False,
    #     help="Active le traitement video pour fallback seulement (h264 270p)"
    # )

    parser.add_argument(
        '--images', action="store_true", required=False,
        help="Processes images and makes thumbnails of files (excluding videos)"
    )
    parser.add_argument(
        '--videos', action="store_true", required=False,
        help="Processes videos and creates video thumbnails"
    )

    args = parser.parse_args()
    __adjust_logging(args)
    return args



class ConfigurationMedia(MilleGrillesBusConfiguration):

    def __init__(self):
        super().__init__()
        self.dir_staging = '/var/opt/millegrilles/staging'
        self.fallback_only = False
        self.image_processing = False
        self.video_processing = False
        # self.filehost_url: Optional[str] = None

    def parse_config(self):
        """
        Conserver l'information de configuration
        :return:
        """
        super().parse_config()

        self.dir_staging = environ.get(ConstantesMessages.ENV_DIR_STAGING) or self.dir_staging
        # self.filehost_url = environ.get(Constantes.ENV_FILEHOST_URL)

    def parse_args(self, args: argparse.Namespace):
        self.image_processing = args.images
        self.video_processing = args.videos

    @staticmethod
    def load():
        # Override
        config = ConfigurationMedia()
        args = _parse_command_line()
        config.parse_config()
        config.parse_args(args)
        return config
