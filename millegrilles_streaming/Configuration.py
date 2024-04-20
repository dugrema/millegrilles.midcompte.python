import asyncio
import os

from typing import Optional

from millegrilles_messages.messages import Constantes as ConstantesMessages
from millegrilles_streaming import Constantes
from millegrilles_messages.MilleGrillesConnecteur import Configuration as ConfigurationAbstract

CONST_BACKUP_PARAMS = [
    Constantes.ENV_DIR_STAGING,
]

CONST_WEB_PARAMS = [
    Constantes.ENV_WEB_PORT,
    ConstantesMessages.ENV_CA_PEM,
    Constantes.PARAM_CERT_PATH,
    Constantes.PARAM_KEY_PATH,
]


class ConfigurationStreaming(ConfigurationAbstract):

    def __init__(self):
        super().__init__()
        self.dir_staging = '/var/opt/millegrilles/staging/streaming'

    def get_params_list(self) -> list:
        params = super().get_params_list()
        params.extend(CONST_BACKUP_PARAMS)
        return params

    def parse_config(self, configuration: Optional[dict] = None):
        """
        Conserver l'information de configuration
        :param configuration:
        :return:
        """
        dict_params = super().parse_config(configuration)

        # Params optionnels
        self.dir_staging = dict_params.get(Constantes.ENV_DIR_STAGING) or self.dir_staging


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


class InformationFuuid:

    def __init__(self, fuuid: str, jwt_token: Optional[str] = None, params: Optional[dict] = None):
        self.fuuid = fuuid
        self.jwt_token = jwt_token
        self.user_id: Optional[str] = None             # User_id, requis pour recuperer les cles
        self.taille: Optional[int] = None               # Taille du fichier
        self.mimetype: Optional[str] = None             # Mimetype du fichier dechiffre
        self.status: Optional[int] = None               # Status du back-end/consignation
        self.position_courante: Optional[int] = None    # Position courante de dechiffrage
        self.path_complet: Optional[str] = None         # Path complet sur disque du fichier dechiffre

        self.ref: Optional[str] = None                  # Fuuid de reference (pour cle)
        self.header: Optional[str] = None               # Header de dechiffrage
        self.nonce: Optional[str] = None                # Nonce de dechiffrage
        self.format: Optional[str] = None               # Format de dechiffrage

        self.event_pret: Optional[asyncio.Event] = None  # Set quand le download est pret

        if params is not None:
            self.set_params(params)

    async def init(self):
        self.event_pret = asyncio.Event()

    def set_params(self, params: dict):
        self.taille = params.get('taille')
        self.mimetype = params.get('mimetype')
        self.status = params.get('status')
        self.user_id = params.get('userId') or params.get('user_id')
        self.ref = params.get('ref')
        self.header = params.get('header')
        self.nonce = params.get('nonce')
        self.format = params.get('format')


    @property
    def est_pret(self):
        if self.event_pret is not None:
            return self.event_pret.is_set()
        return self.path_complet is not None

    def get_params_dechiffrage(self):
        if self.nonce is not None:
            nonce = 'm' + self.nonce  # Ajouter 'm' multibase
        else:
            nonce = self.header
        return {
            'nonce': nonce,
            'format': self.format,
        }
