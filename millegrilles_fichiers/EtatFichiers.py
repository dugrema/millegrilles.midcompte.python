import logging

from typing import Optional

from ssl import SSLContext

from millegrilles_messages.MilleGrillesConnecteur import EtatInstance
from millegrilles_fichiers.Configuration import ConfigurationFichiers


class EtatFichiers(EtatInstance):

    def __init__(self, configuration: ConfigurationFichiers):
        super().__init__(configuration)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

        self.__ssl_context: Optional[SSLContext] = None

        self.__backend: str = 'local'
        self.__topologie: Optional[dict] = None
        self.__primaire: Optional[dict] = None

    async def reload_configuration(self):
        await super().reload_configuration()
        self.__ssl_context = SSLContext()
        self.__ssl_context.load_cert_chain(self.configuration.cert_pem_path, self.configuration.key_pem_path)

    @property
    def ssl_context(self):
        return self.__ssl_context

    @property
    def topologie(self):
        return self.__topologie

    @topologie.setter
    def topologie(self, topologie):
        self.__topologie = topologie

    @property
    def primaire(self):
        return self.primaire

    @primaire.setter
    def primaire(self, primaire):
        self.__primaire = primaire
