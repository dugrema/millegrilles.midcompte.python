import logging

from typing import Optional

from millegrilles.messages.CleCertificat import CleCertificat
from millegrilles.messages.EnveloppeCertificat import EnveloppeCertificat
from millegrilles.midcompte.Configuration import ConfigurationMidcompte


class EtatMidcompte:

    def __init__(self, configuration: ConfigurationMidcompte):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__configuration = configuration

        self.__clecertificat: Optional[CleCertificat] = None
        self.__certificat_millegrille: Optional[EnveloppeCertificat] = None
        self.__password_mq: Optional[str] = None
        self.__password_mongo: Optional[str] = None
        self.__idmg: Optional[str] = None

    async def reload_configuration(self):
        self.__logger.info("Reload configuration sur disque ou dans docker")

        # Charger et verificat cle/certificat
        self.__clecertificat = CleCertificat.from_files(
            self.__configuration.key_pem_path, self.__configuration.cert_pem_path)

        self.__certificat_millegrille = EnveloppeCertificat.from_file(self.__configuration.ca_pem_path)
        self.__idmg = self.__certificat_millegrille.idmg

        with open(self.__configuration.password_mq_path, 'r') as fichier:
            self.__password_mq = fichier.read().strip()

        with open(self.__configuration.password_mongo_path, 'r') as fichier:
            self.__password_mongo = fichier.read().strip()

    @property
    def configuration(self):
        return self.__configuration

    @property
    def idmg(self):
        return self.__idmg

