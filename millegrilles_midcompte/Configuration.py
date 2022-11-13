import argparse
import os

from typing import Optional

from millegrilles_midcompte import Constantes
from millegrilles_messages.messages import Constantes as ConstantesMessages

CONST_INSTANCE_PARAMS = [
    Constantes.PARAM_MONGO_HOSTNAME,
    Constantes.PARAM_MONGO_PORT,
    Constantes.PARAM_MQ_URL,
    Constantes.PARAM_CERT_PATH,
    Constantes.PARAM_KEY_PATH,
    ConstantesMessages.ENV_CA_PEM,
    Constantes.PARAM_MQ_PASSWORD_PATH,
    Constantes.PARAM_MONGO_PASSWORD_PATH,
]

CONST_WEB_PARAMS = [
    Constantes.ENV_WEB_PORT,
    ConstantesMessages.ENV_CA_PEM,
    Constantes.PARAM_CERT_PATH,
    Constantes.PARAM_KEY_PATH,
]


class ConfigurationMidcompte:

    def __init__(self):
        self.mongo_hostname = 'mongo'
        self.mongo_port = 27017
        self.mq_url = 'https://mq:8443'

        self.cert_pem_path = '/run/secrets/pki.midcompte.cert'
        self.key_pem_path = '/run/secrets/pki.midcompte.key'
        self.ca_pem_path = '/run/secrets/pki.millegrille'
        self.password_mq_path = '/run/secrets/passwd.mqadmin.txt'
        self.password_mongo_path = '/run/secrets/passwd.mongo.txt'

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

        mq_url = dict_params.get(Constantes.PARAM_MQ_URL)
        if mq_url == '':
            self.mq_url = None
        else:
            self.mq_url = mq_url or self.mq_url

        self.mongo_hostname = dict_params.get(Constantes.PARAM_MONGO_HOSTNAME) or self.mongo_hostname
        self.mongo_port = dict_params.get(Constantes.PARAM_MONGO_PORT) or self.mongo_port
        self.cert_pem_path = dict_params.get(Constantes.PARAM_CERT_PATH) or self.cert_pem_path
        self.key_pem_path = dict_params.get(Constantes.PARAM_KEY_PATH) or self.key_pem_path
        self.ca_pem_path = dict_params.get(ConstantesMessages.ENV_CA_PEM) or self.ca_pem_path
        self.password_mq_path = dict_params.get(Constantes.PARAM_MQ_PASSWORD_PATH) or self.password_mq_path
        self.password_mongo_path = dict_params.get(Constantes.PARAM_MONGO_PASSWORD_PATH) or self.password_mongo_path

    def desactiver_mq(self):
        self.mq_url = None
        self.password_mq_path = None


class ConfigurationWeb:

    def __init__(self):
        self.ca_pem_path = '/run/secrets/pki.millegrille'
        self.web_cert_pem_path = '/run/secrets/pki.midcompte.cert'
        self.web_key_pem_path = '/run/secrets/pki.midcompte.key'
        self.port = 2444

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
        self.web_cert_pem_path = dict_params.get(Constantes.PARAM_CERT_PATH) or self.web_cert_pem_path
        self.web_key_pem_path = dict_params.get(Constantes.PARAM_KEY_PATH) or self.web_key_pem_path
        self.port = int(dict_params.get(Constantes.ENV_WEB_PORT) or self.port)
