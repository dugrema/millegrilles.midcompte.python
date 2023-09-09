import asyncio
import os

from typing import Optional

from millegrilles_messages.messages import Constantes as ConstantesMessages
from millegrilles_fichiers import Constantes
from millegrilles_messages.MilleGrillesConnecteur import Configuration as ConfigurationAbstract

CONST_FICHIERS_PARAMS = [
    Constantes.ENV_DIR_CONSIGNATION,
    Constantes.ENV_PATH_KEY_SSH_ED25519,
    Constantes.ENV_PATH_KEY_SSH_RSA
]

CONST_WEB_PARAMS = [
    Constantes.ENV_WEB_PORT,
    ConstantesMessages.ENV_CA_PEM,
    Constantes.PARAM_CERT_PATH,
    Constantes.PARAM_KEY_PATH,
]


class ConfigurationFichiers(ConfigurationAbstract):

    def __init__(self):
        super().__init__()
        self.dir_consignation = '/var/opt/millegrilles/consignation'
        self.path_key_ssh_ed25519 = '/run/secrets/sftp.ed25519.key.pem'
        self.path_key_ssh_rsa = '/run/secrets/sftp.rsa.key.pem'

    def get_params_list(self) -> list:
        params = super().get_params_list()
        params.extend(CONST_FICHIERS_PARAMS)
        return params

    def parse_config(self, configuration: Optional[dict] = None):
        """
        Conserver l'information de configuration
        :param configuration:
        :return:
        """
        dict_params = super().parse_config(configuration)

        # Params optionnels
        self.dir_consignation = dict_params.get(Constantes.ENV_DIR_CONSIGNATION) or self.dir_consignation
        self.path_key_ssh_ed25519 = dict_params.get(Constantes.ENV_PATH_KEY_SSH_ED25519) or self.path_key_ssh_ed25519
        self.path_key_ssh_rsa = dict_params.get(Constantes.ENV_PATH_KEY_SSH_RSA) or self.path_key_ssh_rsa


class ConfigurationWeb:

    def __init__(self):
        self.ca_pem_path = '/run/secrets/pki.millegrille.pem'
        self.web_cert_pem_path = '/run/secrets/cert.pem'
        self.web_key_pem_path = '/run/secrets/key.pem'
        self.port = 443

    def get_env(self) -> dict:
        """
        Extrait l'information pertinente pour pika de os.environ
        :return: Configuration dict
        """
        config = dict()
        for opt_param in CONST_WEB_PARAMS:
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
        self.web_cert_pem_path = dict_params.get(ConstantesMessages.ENV_CERT_PEM) or self.web_cert_pem_path
        self.web_key_pem_path = dict_params.get(ConstantesMessages.ENV_KEY_PEM) or self.web_key_pem_path
        self.port = int(dict_params.get(Constantes.ENV_WEB_PORT) or self.port)

