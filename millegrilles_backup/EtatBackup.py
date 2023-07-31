import logging

from typing import Optional

from ssl import SSLContext

from millegrilles_messages.MilleGrillesConnecteur import EtatInstance
from millegrilles_backup.Configuration import ConfigurationBackup


class EtatBackup(EtatInstance):

    def __init__(self, configuration: ConfigurationBackup):
        super().__init__(configuration)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

        self.__ssl_context: Optional[SSLContext] = None

    async def reload_configuration(self):
        await super().reload_configuration()
        self.__ssl_context = SSLContext()
        self.__ssl_context.load_cert_chain(self.configuration.cert_pem_path, self.configuration.key_pem_path)

    @property
    def ssl_context(self):
        return self.__ssl_context
