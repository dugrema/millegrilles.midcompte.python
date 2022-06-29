import argparse
import os

from typing import Optional

from millegrilles_midcompte import Constantes
from millegrilles_messages.messages import Constantes as ConstantesMessages

CONST_BACKUP_PARAMS = [
    ConstantesMessages.ENV_CA_PEM,
    Constantes.PARAM_CERT_PATH,
    Constantes.PARAM_KEY_PATH,
]


class ConfigurationBackup:

    def __init__(self):
        self.ca_pem_path = '/var/opt/millegrilles/configuration/pki.millegrille.cert'
        self.web_cert_pem_path: Optional[str] = None
        self.web_key_pem_path: Optional[str] = None

    def get_env(self) -> dict:
        """
        Extrait l'information pertinente de os.environ
        :return: Configuration dict
        """
        config = dict()
        for opt_param in CONST_BACKUP_PARAMS:
            value = os.environ.get(opt_param)
            if value is not None:
                config[opt_param] = value

        return config

    def parse_config(self, configuration: Optional[dict] = None):
        """
        Conserver l'information de configuration
        :param configuration:
        :return:
        """
        dict_params = self.get_env()
        if configuration is not None:
            dict_params.update(configuration)

        self.ca_pem_path = dict_params.get(ConstantesMessages.ENV_CA_PEM) or self.ca_pem_path
        self.web_cert_pem_path = dict_params.get(Constantes.PARAM_CERT_PATH) or self.web_cert_pem_path
        self.web_key_pem_path = dict_params.get(Constantes.PARAM_KEY_PATH) or self.web_key_pem_path