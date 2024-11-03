import asyncio
import logging
import ssl
from ssl import SSLContext

from asyncio import wait
from typing import Optional

from OpenSSL.SSL import VERIFY_NONE

from millegrilles_messages.messages.CleCertificat import CleCertificat
from millegrilles_messages.messages.EnveloppeCertificat import EnveloppeCertificat
from millegrilles_messages.messages.FormatteurMessages import SignateurTransactionSimple, FormatteurMessageMilleGrilles
from millegrilles_messages.messages.ValidateurCertificats import ValidateurCertificatCache
from millegrilles_messages.messages.ValidateurMessage import ValidateurMessage
from millegrilles_messages.messages.MessagesModule import MessageProducerFormatteur
from millegrilles_media.Configuration import ConfigurationMedia


class EtatMedia:

    def __init__(self, configuration: ConfigurationMedia, video_desactive: bool):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__configuration = configuration
        self.__video_desactive = video_desactive

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

        # self.__instance_id_consgination = None
        # self.__url_consgination = None
        self.__filehost: Optional[dict] = None
        self.__filehost_url: Optional[str] = None
        self.__tls_method: Optional[str] = None
        self.__ssl_context: Optional[SSLContext] = None

    def __configure_ssl(self):
        if self.__tls_method is None:
            return  # Nothing to do

        if self.__tls_method == 'millegrille':
            # Internal mode
            ssl_context = SSLContext()
            ssl_context.load_cert_chain(self.__configuration.cert_pem_path, self.__configuration.key_pem_path)
        elif self.__tls_method == 'external':
            # Internet mode
            ssl_context = ssl.create_default_context()
        elif self.__tls_method == 'nocheck':
            # No certificate check (insecure) - also need to use verify=False on requests
            ssl_context = ssl.create_default_context()
        else:
            raise ValueError('Unknown TLS check method: %s' % self.__tls_method)

        self.__ssl_context = ssl_context

    async def reload_configuration(self):
        self.__logger.info("Reload configuration sur disque ou dans docker")

        self.__mq_host = self.__configuration.mq_host or self.__configuration_json.get('mq_host') or 'localhost'
        self.__mq_port = self.__configuration.mq_port or self.__configuration_json.get('mq_port') or 5673

        self.__certificat_millegrille = EnveloppeCertificat.from_file(self.__configuration.ca_pem_path)
        self.__validateur_certificats = ValidateurCertificatCache(self.__certificat_millegrille)

        # Charger et verificat cle/certificat
        self.__clecertificat = CleCertificat.from_files(
            self.__configuration.key_pem_path, self.__configuration.cert_pem_path)

        self.__configure_ssl()

        if self.__clecertificat is not None:
            idmg = self.__clecertificat.enveloppe.idmg

            # Valider le certificat en memoire
            try:
                await self.__validateur_certificats.valider(self.__clecertificat.enveloppe.chaine_pem())

                signateur = SignateurTransactionSimple(self.__clecertificat)
                self.__formatteur_message = FormatteurMessageMilleGrilles(idmg, signateur, self.__certificat_millegrille)
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
            try:
                await self.charger_url_consignation(rabbitmq_dao)
            except asyncio.TimeoutError:
                self.__logger.info("Timeout error pour rafraichir url consignation")
            except Exception:
                self.__logger.exception("Exception rafraichissement url consignation")

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass

    async def charger_url_consignation(self, rabbitmq_dao):
        producer = None
        for i in range(0, 3):
            producer = rabbitmq_dao.get_producer()
            if producer:
                break
            await asyncio.sleep(1)

        if producer is None:
            self.__logger.warning("charger_url_consignation Producer mq n'est pas encore charge - skip")
            return

        await asyncio.wait_for(producer.producer_pret().wait(), timeout=60)

        reponse = await producer.executer_requete(
            dict(), 'CoreTopologie', 'getFilehostForInstance', exchange="1.public")
        # reponse = await producer.executer_requete(
        #     dict(), 'CoreTopologie', 'getConsignationFichiers', exchange="2.prive")

        try:
            filehost_response = reponse.parsed
            filehost = filehost_response['filehost']
            self.preparer_filehost_url_context(filehost)
            self.__filehost = filehost
        except Exception as e:
            self.__logger.exception("Erreur chargement filehost")

    def preparer_filehost_url_context(self, filehost: dict):
        if filehost.get('instance_id') == self.instance_id and filehost.get('url_internal') is not None:
            self.__filehost_url = filehost['url_internal']
            # Connecter avec certificat interne
        elif filehost.get('url_external') is not None and filehost.get('tls_external') is not None:
            self.__filehost_url = filehost['url_external']
            self.__tls_method = filehost['tls_external']
        else:
            raise ValueError('No acceptable URL')

        # Apply configuration
        self.__configure_ssl()

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
    def video_desactive(self):
        return self.__video_desactive

    @property
    def ssl_context(self):
        return self.__ssl_context

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
    def instance_id_consignation(self) -> str:
        raise NotImplementedError('obsolete')
        # return self.__instance_id_consgination

    @property
    def url_consignation(self) -> str:
        raise NotImplementedError('obsolete')
        # return self.__url_consignation

    @property
    def filehost(self):
        return self.__filehost

    @property
    def filehost_url(self):
        return self.__filehost_url

    @property
    def tls_method(self):
        return self.__tls_method

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
