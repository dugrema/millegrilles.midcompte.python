import logging

from typing import Optional

from pymongo import MongoClient

from millegrilles_messages.messages.EnveloppeCertificat import EnveloppeCertificat


class EntretienMongoDb:

    def __init__(self, etat_midcompte):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__etat_midcompte = etat_midcompte

        self.__passwd_mongodb: Optional[str] = None
        self.__client_mongo: Optional[MongoClient] = None

    async def connecter_mongo(self):
        configuration_mongo = {
            'host': self.__etat_midcompte.configuration.mongo_hostname,
            'port': self.__etat_midcompte.configuration.mongo_port,
            'username': 'admin',
            'password': self.__etat_midcompte.password_mongo,
            'tls': True,
            # 'authSource': '',
            'tlsCertificateKeyFile': self.__etat_midcompte.configuration.key_pem_path,
            'tlsCAFile': self.__etat_midcompte.configuration.ca_pem_path,
        }
        self.__logger.debug("Connexion a MongoDB")
        client_mongo = MongoClient(**configuration_mongo)
        self.__logger.debug("Verify if connection established")
        client_mongo.admin.command('ismaster')
        self.__logger.info("Connexion MongoDB etablie")
        self.__client_mongo = client_mongo

    async def entretien(self):
        self.__logger.debug("entretien debut")

        try:
            if self.__client_mongo is None:
                await self.connecter_mongo()
        except Exception as e:
            self.__logger.exception("Erreur verification MongoDB https")

        self.__logger.debug("entretien fin")

    async def ajouter_compte(self, info: dict):
        self.__logger.debug("Ajouter compte dans MongoDB: %s" % info)
        enveloppe: EnveloppeCertificat = info['certificat']
        idmg = enveloppe.idmg

        # subject = enveloppe.subject_rfc4514_string_mq()
        subject = enveloppe.subject_rfc4514_string()
        self.__logger.info("Creation compte MQ pour %s" % subject)

        commande = {
            'createUser': subject,
            'roles': [{
                'role': 'readWrite',
                'db': idmg,
            }]
        }

        self.__logger.debug("Creation compte Mongo : %s", commande)
        external_db = self.__client_mongo.get_database('$external')
        external_db.command(commande)
