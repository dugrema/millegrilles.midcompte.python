import logging

from typing import Optional

from millegrilles_messages.messages.CleCertificat import CleCertificat
from millegrilles_messages.messages.EnveloppeCertificat import EnveloppeCertificat
from millegrilles_midcompte.Configuration import ConfigurationMidcompte


class EtatMidcompte:

    def __init__(self, configuration: ConfigurationMidcompte):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__configuration = configuration

        self.__clecertificat: Optional[CleCertificat] = None
        self.__certificat_millegrille: Optional[EnveloppeCertificat] = None
        self.__password_mq: Optional[str] = None
        self.__password_mongo: Optional[str] = None
        self.__idmg: Optional[str] = None

        self.__listeners_actions = list()

    async def reload_configuration(self):
        self.__logger.info("Reload configuration sur disque ou dans docker")

        # Charger et verificat cle/certificat
        self.__clecertificat = CleCertificat.from_files(
            self.__configuration.key_pem_path, self.__configuration.cert_pem_path)

        self.__certificat_millegrille = EnveloppeCertificat.from_file(self.__configuration.ca_pem_path)
        self.__idmg = self.__certificat_millegrille.idmg

        with open(self.__configuration.password_mongo_path, 'r') as fichier:
            self.__password_mongo = fichier.read().strip()

        if self.__configuration.mq_url == '':
            self.__configuration.desactiver_mq()
            self.__logger.warning("Desactiver gestion MQ (url == '')")
            self.__configuration.desactiver_mq()
        else:
            try:
                with open(self.__configuration.password_mq_path, 'r') as fichier:
                    self.__password_mq = fichier.read().strip()
            except FileNotFoundError:
                self.__logger.warning("Pas de mot de passe MQ (FileNotFound), desactiver gestion MQ")
                self.__configuration.desactiver_mq()

    async def ajouter_compte(self, info: dict):
        self.__logger.info("Ajouter compte : %s" % info)
        tous_succes = True
        for listener in self.__listeners_actions:
            if hasattr(listener, 'ajouter_compte') is True:
                try:
                    await listener.ajouter_compte(info)
                except:
                    self.__logger.exception("Erreur ajout compte")
                    tous_succes = False

        if tous_succes is False:
            raise Exception('Erreur ajout au moins un compte')

    def ajouter_listener(self, listener):
        self.__listeners_actions.append(listener)

    @property
    def configuration(self):
        return self.__configuration

    @property
    def password_mongo(self):
        return self.__password_mongo

    @property
    def idmg(self):
        return self.__idmg

