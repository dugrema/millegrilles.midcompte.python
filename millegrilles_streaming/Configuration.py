import argparse
import os
import logging

from millegrilles_messages.bus.BusConfiguration import MilleGrillesBusConfiguration
from millegrilles_messages.messages import Constantes as ConstantesMessages
from millegrilles_streaming import Constantes as StreamingConstants

LOGGING_NAMES = [__name__, 'millegrilles_messages', 'millegrilles_streaming']


def __adjust_logging(args: argparse.Namespace):
    logging.basicConfig()
    if args.verbose is True:
        for log in LOGGING_NAMES:
            logging.getLogger(log).setLevel(logging.DEBUG)
    else:
        for log in LOGGING_NAMES:
            logging.getLogger(log).setLevel(logging.INFO)


def _parse_command_line():
    parser = argparse.ArgumentParser(description="File and media streaming for MilleGrilles")
    parser.add_argument(
        '--verbose', action="store_true", required=False,
        help="More logging"
    )

    args = parser.parse_args()
    __adjust_logging(args)
    return args


class StreamingConfiguration(MilleGrillesBusConfiguration):

    def __init__(self):
        super().__init__()
        self.dir_staging = '/var/opt/millegrilles/staging/streaming'
        self.web_cert_pem_path = '/var/opt/millegrilles/secrets/pki.web.cert'
        self.web_key_pem_path = '/var/opt/millegrilles/secrets/pki.web.key'
        self.web_port = 2443

    def parse_config(self):
        """
        Conserver l'information de configuration
        :return:
        """
        super().parse_config()

        self.dir_staging = os.environ.get(ConstantesMessages.ENV_DIR_STAGING) or self.dir_staging
        self.web_cert_pem_path = os.environ.get(StreamingConstants.ENV_WEB_CERT_PEM) or self.web_cert_pem_path
        self.web_key_pem_path = os.environ.get(StreamingConstants.ENV_WEB_KEY_PEM) or self.web_key_pem_path
        try:
            self.web_port = int(os.environ.get(StreamingConstants.ENV_WEB_PORT))
        except (TypeError, ValueError):
            pass

    def parse_args(self, args: argparse.Namespace):
        pass

    @staticmethod
    def load():
        # Override
        config = StreamingConfiguration()
        args = _parse_command_line()
        config.parse_config()
        config.parse_args(args)
        return config


# class ConfigurationWeb:
#
#     def __init__(self):
#         self.ca_pem_path = '/run/secrets/pki.millegrille.pem'
#         self.web_cert_pem_path = '/run/secrets/cert.pem'
#         self.web_key_pem_path = '/run/secrets/key.pem'
#         self.port = 1443
#
#     def get_env(self) -> dict:
#         """
#         Extrait l'information pertinente pour pika de os.environ
#         :return: Configuration dict
#         """
#         config = dict()
#         for opt_param in CONST_WEB_PARAMS:
#             value = os.environ.get(opt_param)
#             if value is not None:
#                 config[opt_param] = value
#
#         return config
#
#     def parse_config(self, configuration: Optional[dict] = None):
#         """
#         Conserver l'information de configuration
#         :param configuration:
#         :return:
#         """
#         dict_params = self.get_env()
#         if configuration is not None:
#             dict_params.update(configuration)
#
#         self.ca_pem_path = dict_params.get(ConstantesMessages.ENV_CA_PEM) or self.ca_pem_path
#         self.web_cert_pem_path = dict_params.get(ConstantesMessages.ENV_CERT_PEM) or self.web_cert_pem_path
#         self.web_key_pem_path = dict_params.get(ConstantesMessages.ENV_KEY_PEM) or self.web_key_pem_path
#         self.port = int(dict_params.get(Constantes.ENV_WEB_PORT) or self.port)
