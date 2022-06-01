import argparse
import os

from typing import Optional

from millegrilles_certissuer import Constantes

CONST_INSTANCE_PARAMS = [
    Constantes.CERTISSUER_PATH,
    Constantes.PARAM_INSTANCE_ID,
    Constantes.PARAM_DUREE_CERTIFICAT_JOURS,
    Constantes.PARAM_DUREE_CERTIFICAT_MINUTES,
]

CONST_WEB_PARAMS = [
    Constantes.ENV_WEB_PORT,
]


class ConfigurationCertissuer:

    def __init__(self):
        self.path_certissuer = '/var/opt/millegrilles/certissuer'
        self.instance_id: Optional[str] = None
        self.duree_certificats_jours: Optional[int] = None
        self.duree_certificats_minutes: Optional[int] = None

    def get_env(self) -> dict:
        """
        Extrait l'information pertinente pour pika de os.environ
        :return: Configuration dict
        """
        config = dict()
        for opt_param in CONST_INSTANCE_PARAMS:
            value = os.environ.get(opt_param)
            if value is not None:
                config[opt_param] = value

        return config

    def parse_config(self, args: argparse.Namespace, configuration: Optional[dict] = None):
        """
        Conserver l'information de configuration
        :param args:
        :param configuration:
        :return:
        """
        dict_params = self.get_env()
        if configuration is not None:
            dict_params.update(configuration)

        self.path_certissuer = dict_params.get(Constantes.CERTISSUER_PATH) or self.path_certissuer
        self.instance_id = dict_params[Constantes.PARAM_INSTANCE_ID]
        self.duree_certificats_jours = dict_params.get(Constantes.PARAM_DUREE_CERTIFICAT_JOURS) or self.duree_certificats_jours
        self.duree_certificats_minutes = dict_params.get(Constantes.PARAM_DUREE_CERTIFICAT_MINUTES) or self.duree_certificats_minutes


class ConfigurationWeb:

    def __init__(self):
        self.port = '2080'

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

        self.port = int(dict_params.get(Constantes.ENV_WEB_PORT) or self.port)
