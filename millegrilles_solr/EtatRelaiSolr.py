import logging

from asyncio import wait
from typing import Optional

from millegrilles_messages.messages.CleCertificat import CleCertificat
from millegrilles_messages.messages.EnveloppeCertificat import EnveloppeCertificat
from millegrilles_messages.messages.FormatteurMessages import SignateurTransactionSimple, FormatteurMessageMilleGrilles
from millegrilles_messages.messages.ValidateurCertificats import ValidateurCertificatCache
from millegrilles_messages.messages.ValidateurMessage import ValidateurMessage
from millegrilles_messages.messages.MessagesModule import MessageProducerFormatteur
from millegrilles_solr.Configuration import ConfigurationRelaiSolr


class EtatRelaiSolr:

    def __init__(self, configuration: ConfigurationRelaiSolr):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__configuration = configuration

        self.__configuration_json: Optional[dict] = None

        self.__instance_id: Optional[str] = None
        self.__mq_host: Optional[str] = None
        self.__mq_port: Optional[int] = None
        self.__clecertificat: Optional[CleCertificat] = None
        self.__certificat_millegrille: Optional[EnveloppeCertificat] = None

        self.__listeners_actions = list()

        self.__formatteur_message: Optional[FormatteurMessageMilleGrilles] = None
        self.__validateur_certificats: Optional[ValidateurCertificatCache] = None
        self.__validateur_message: Optional[ValidateurMessage] = None

        # self.__stop_event: Optional[Event] = None
        self.__producer: Optional[MessageProducerFormatteur] = None
        self.__partition: Optional[str] = None

        self.__url_consignation = None

    async def reload_configuration(self):
        self.__logger.info("Reload configuration sur disque ou dans docker")

        self.__mq_host = self.__configuration.mq_host or self.__configuration_json.get('mq_host') or 'localhost'
        self.__mq_port = self.__configuration.mq_port or self.__configuration_json.get('mq_port') or 5673

        self.__certificat_millegrille = EnveloppeCertificat.from_file(self.__configuration.ca_pem_path)
        self.__validateur_certificats = ValidateurCertificatCache(self.__certificat_millegrille)

        # Charger et verificat cle/certificat
        self.__clecertificat = CleCertificat.from_files(
            self.__configuration.key_pem_path, self.__configuration.cert_pem_path)

        if self.__clecertificat is not None:
            idmg = self.__clecertificat.enveloppe.idmg

            # Valider le certificat en memoire
            try:
                await self.__validateur_certificats.valider(self.__clecertificat.enveloppe.chaine_pem())

                signateur = SignateurTransactionSimple(self.__clecertificat)
                self.__formatteur_message = FormatteurMessageMilleGrilles(idmg, signateur)
                self.__validateur_message = ValidateurMessage(self.__validateur_certificats)
            except Exception:
                self.__logger.exception("Certificat invalide/expire")
                self.__formatteur_message = None
                self.__validateur_message = None

        for listener in self.__listeners_actions:
            await listener()

    async def run(self, stop_event, rabbitmq_dao):
        while stop_event.is_set() is False:

            # Charger configuration consignation via topologie
            await self.charger_url_consignation(rabbitmq_dao)

            await wait([stop_event.wait()], timeout=30)

    async def charger_url_consignation(self, rabbitmq_dao):
        producer = rabbitmq_dao.get_producer()
        await wait([producer.producer_pret().wait()], timeout=60)

        reponse = await producer.executer_requete(
            dict(), 'CoreTopologie', 'getConsignationFichiers', exchange="2.prive")

        try:
            self.__url_consignation = reponse.parsed['consignation_url']
        except Exception as e:
            self.__logger.exception("Erreur chargement URL consignation")

    def ajouter_listener(self, listener):
        self.__listeners_actions.append(listener)

    async def fermer(self):
        for listener in self.__listeners_actions:
            await listener(fermer=True)

    async def verifier_certificat_expire(self):
        enveloppe = self.clecertificat.enveloppe
        try:
            enveloppe = await self.__validateur_certificats.valider(enveloppe.chaine_pem())
            return enveloppe is None
        except:
            self.__logger.warning("Le certificat local est expire")
            return True

    @property
    def configuration(self):
        return self.__configuration

    @property
    def clecertificat(self):
        return self.__clecertificat

    @property
    def instance_id(self):
        return self.__instance_id

    @property
    def mq_host(self):
        return self.__mq_host

    @property
    def mq_port(self):
        return self.__mq_port

    @property
    def url_consignation(self) -> str:
        return self.__url_consignation

    def set_producer(self, producer: MessageProducerFormatteur):
        self.__producer = producer

    @property
    def producer(self):
        return self.__producer

    def set_partition(self, partition: str):
        self.__partition = partition

    @property
    def partition(self):
        return self.__partition

    @property
    def formatteur_message(self):
        return self.__formatteur_message
