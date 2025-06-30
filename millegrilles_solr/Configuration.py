import argparse
import logging

from os import environ
from typing import Optional

from millegrilles_messages.bus.BusConfiguration import MilleGrillesBusConfiguration
from millegrilles_messages.messages import Constantes as ConstantesMessages
from millegrilles_solr import Constantes

ENV_PROCESS_COUNT = "PROCESS_COUNT"
DEFAULT_PROCESS_COUNT = 1

CONST_RELAISOLR_PARAMS = [
    # ConstantesMessages.ENV_CA_PEM,
    # ConstantesMessages.ENV_CERT_PEM,
    # ConstantesMessages.ENV_KEY_PEM,
    # ConstantesMessages.ENV_MQ_HOSTNAME,
    # ConstantesMessages.ENV_MQ_PORT,
    ConstantesMessages.ENV_SOLR_URL,
]

LOGGING_NAMES = [__name__, 'solrdao', 'millegrilles_messages', 'millegrilles_solr']


def __adjust_logging(args: argparse.Namespace):
    logging.basicConfig()
    if args.verbose is True:
        for log in LOGGING_NAMES:
            logging.getLogger(log).setLevel(logging.DEBUG)
    else:
        for log in LOGGING_NAMES:
            logging.getLogger(log).setLevel(logging.INFO)


def _parse_command_line():
    parser = argparse.ArgumentParser(description="Solr indexing for MilleGrilles")
    parser.add_argument(
        '--verbose', action="store_true", required=False,
        help="More logging"
    )
    parser.add_argument(
        '--scripts', action="store_true", required=False,
        help="Execute scripts"
    )

    args = parser.parse_args()
    __adjust_logging(args)
    return args


class ConfigurationRelaiSolr(MilleGrillesBusConfiguration):

    def __init__(self):
        super().__init__()
        self.dir_staging = '/var/opt/millegrilles/staging'
        self.filehost_url: Optional[str] = None
        self.solr_url = 'https://solr:8983'
        self.nom_collection_fichiers = 'fichiers'
        self.process_count = DEFAULT_PROCESS_COUNT

    def parse_config(self):
        """
        Conserver l'information de configuration
        :return:
        """
        super().parse_config()

        self.dir_staging = environ.get(ConstantesMessages.ENV_DIR_STAGING) or self.dir_staging
        self.filehost_url = environ.get(Constantes.ENV_FILEHOST_URL)
        self.solr_url = environ.get(ConstantesMessages.ENV_SOLR_URL) or self.solr_url

        process_count = environ.get(ENV_PROCESS_COUNT)
        if process_count:
            self.process_count = int(process_count)

    def parse_args(self, args: argparse.Namespace):
        pass

    @staticmethod
    def load():
        # Override
        config = ConfigurationRelaiSolr()
        args = _parse_command_line()
        config.parse_config()
        config.parse_args(args)
        return config


# class ConfigurationRelaiSolr:
#
#     def __init__(self):
#         self.ca_pem_path = '/var/opt/millegrilles/configuration/pki.millegrille.cert'
#         self.cert_pem_path = '/var/opt/millegrilles/secrets/pki.solr_relai.cert'
#         self.key_pem_path = '/var/opt/millegrilles/secrets/pki.solr_relai.cle'
#         self.mq_host = 'localhost'
#         self.mq_port = 5673
#         self.solr_url = 'https://solr:8983'
#         self.nom_collection_fichiers = 'fichiers'
#         self.nom_collection_messages = 'messages'
#
#     def get_env(self) -> dict:
#         """
#         Extrait l'information pertinente de os.environ
#         :return: Configuration dict
#         """
#         config = dict()
#         for opt_param in CONST_RELAISOLR_PARAMS:
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
#         self.cert_pem_path = dict_params.get(ConstantesMessages.ENV_CERT_PEM) or self.cert_pem_path
#         self.key_pem_path = dict_params.get(ConstantesMessages.ENV_KEY_PEM) or self.key_pem_path
#         self.mq_host = dict_params.get(ConstantesMessages.ENV_MQ_HOSTNAME) or self.mq_host
#         self.mq_port = dict_params.get(ConstantesMessages.ENV_MQ_PORT) or self.mq_port
#
#         self.solr_url = dict_params.get(ConstantesMessages.ENV_SOLR_URL) or self.solr_url
