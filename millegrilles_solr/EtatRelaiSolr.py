import asyncio
import logging
import ssl

from typing import Optional
from ssl import SSLContext
from urllib.parse import urljoin

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

        self.__stop_event: Optional[asyncio.Event] = None

        self.__filehost: Optional[dict] = None
        self.__url_consignation = None
        self.__filehost_url: Optional[str] = None
        self.__tls_method: Optional[str] = None

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

            self.__instance_id = self.__clecertificat.enveloppe.subject_common_name

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

        self.__configure_ssl()  # Reload TLS certificates

        for listener in self.__listeners_actions:
            await listener()

    async def run(self, stop_event):
        self.__stop_event = stop_event
        while stop_event.is_set() is False:

            # Charger configuration consignation via topologie
            await self.charger_filehost()

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass

    async def charger_filehost(self):
        producer = None
        for i in range(0, 6):
            producer = self.producer
            if producer is not None:
                break
            try:
                await asyncio.wait_for(self.__stop_event.wait(), 0.5)
                return  # Stopping
            except asyncio.TimeoutError:
                pass

        if producer is None:
            raise Exception('producer pas pret')

        await asyncio.wait_for(producer.producer_pret().wait(), 30)

        reponse = await producer.executer_requete(
            dict(), 'CoreTopologie', 'getFilehostForInstance', exchange="1.public")

        try:
            filehost_response = reponse.parsed
            filehost = filehost_response['filehost']
            self.preparer_filehost_url_context(filehost)
            self.__filehost = filehost
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

    def preparer_filehost_url_context(self, filehost: dict):
        local_instance_id = self.__instance_id
        if filehost.get('instance_id') == local_instance_id and filehost.get('url_internal') is not None:
            self.__filehost_url = urljoin(filehost['url_internal'], '/filehost')
            self.__tls_method = 'millegrille'
            # Connecter avec certificat interne
        elif filehost.get('url_external') is not None and filehost.get('tls_external') is not None:
            self.__filehost_url = urljoin(filehost['url_external'], '/filehost')
            self.__tls_method = filehost['tls_external']
        else:
            raise ValueError('No acceptable URL')

        # Apply configuration
        self.__configure_ssl()

    def __configure_ssl(self):
        if self.__tls_method is None:
            return  # Nothing to do

        if self.__tls_method == 'millegrille':
            # Internal mode
            ssl_context = SSLContext()
            ssl_context.load_cert_chain(self.configuration.cert_pem_path, self.configuration.key_pem_path)
        elif self.__tls_method == 'external':
            # Internet mode
            ssl_context = ssl.create_default_context()
        elif self.__tls_method == 'nocheck':
            # No certificate check (insecure) - also need to use verify=False on requests
            ssl_context = ssl.create_default_context()
        else:
            raise ValueError('Unknown TLS check method: %s' % self.__tls_method)

        self.__ssl_context = ssl_context

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
    def url_filehost(self) -> str:
        # return self.__url_consignation
        return self.__filehost_url

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
