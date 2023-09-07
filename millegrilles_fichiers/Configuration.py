import asyncio
import os

from typing import Optional

from millegrilles_messages.messages import Constantes as ConstantesMessages
from millegrilles_fichiers import Constantes
from millegrilles_messages.MilleGrillesConnecteur import Configuration as ConfigurationAbstract

CONST_FICHIERS_PARAMS = [
    Constantes.ENV_DIR_CONSIGNATION,
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


class InformationFuuid:

    def __init__(self, fuuid: str):
        self.fuuid = fuuid
        self.taille: Optional[int] = None               # Taille du fichier
        self.path_complet: Optional[str] = None         # Path complet sur disque du fichier dechiffre

        # if params is not None:
        #     self.set_params(params)

    @staticmethod
    def resolve_fuuid(config: ConfigurationFichiers, fuuid: str):
        """ Trouver le path local du fichier par son fuuid. """
        info = InformationFuuid(fuuid)
        return info

    # def set_params(self, params: dict):
    #     self.taille = params.get('taille')
    #     self.status = params.get('status')
    #     self.user_id = params.get('userId') or params.get('user_id')
