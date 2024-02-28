import asyncio
import os

from typing import Optional

from millegrilles_messages.messages import Constantes as ConstantesMessages
from millegrilles_fichiers import Constantes
from millegrilles_messages.MilleGrillesConnecteur import Configuration as ConfigurationAbstract

CONST_FICHIERS_PARAMS = [
    Constantes.ENV_DIR_CONSIGNATION,
    Constantes.ENV_DIR_STAGING,
    Constantes.ENV_DIR_DATA,
    Constantes.ENV_PATH_KEY_SSH_ED25519,
    Constantes.ENV_PATH_KEY_SSH_RSA,
    Constantes.ENV_SEM_READ,
    Constantes.ENV_SEM_WRITE,
    Constantes.ENV_SEM_BACKUP,
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
        self.dir_staging = '/var/opt/millegrilles/staging/consignation'
        self.dir_data = '/var/opt/millegrilles/consignation/data'
        self.path_key_ssh_ed25519 = '/run/secrets/sftp.ed25519.key.pem'
        self.path_key_ssh_rsa = '/run/secrets/sftp.rsa.key.pem'
        self.sem_read_count = 30
        self.sem_write_count = 5
        self.sem_backup_count = 2

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
        self.dir_staging = dict_params.get(Constantes.ENV_DIR_STAGING) or self.dir_staging
        self.dir_data = dict_params.get(Constantes.ENV_DIR_DATA) or self.dir_data
        self.path_key_ssh_ed25519 = dict_params.get(Constantes.ENV_PATH_KEY_SSH_ED25519) or self.path_key_ssh_ed25519
        self.path_key_ssh_rsa = dict_params.get(Constantes.ENV_PATH_KEY_SSH_RSA) or self.path_key_ssh_rsa

        self.sem_read_count = int(dict_params.get(Constantes.ENV_SEM_READ) or self.sem_read_count)
        self.sem_write_count = int(dict_params.get(Constantes.ENV_SEM_WRITE) or self.sem_write_count)
        self.sem_backup_count = int(dict_params.get(Constantes.ENV_SEM_BACKUP) or self.sem_backup_count)


class ConfigurationWeb:

    def __init__(self):
        self.ca_pem_path = '/run/secrets/pki.millegrille.pem'
        self.web_cert_pem_path = '/run/secrets/cert.pem'
        self.web_key_pem_path = '/run/secrets/key.pem'
        self.port = 1443

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

