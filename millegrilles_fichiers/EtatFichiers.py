import asyncio
import logging
import pathlib
import sqlite3
import multibase

from typing import Optional

from ssl import SSLContext

from cryptography.hazmat.primitives import serialization as crypto_serialization

from millegrilles_messages.chiffrage.DechiffrageUtils import dechiffrer_document_secrete
from millegrilles_messages.MilleGrillesConnecteur import EtatInstance
from millegrilles_fichiers import Constantes
from millegrilles_fichiers.Configuration import ConfigurationFichiers
from millegrilles_fichiers.SQLiteDao import SQLiteConnection, SQLiteLocks


class EtatFichiers(EtatInstance):

    def __init__(self, configuration: ConfigurationFichiers, sqlite_locks: SQLiteLocks):
        super().__init__(configuration)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

        self.__url_consignation_primaire: Optional[str] = None

        self.__ssl_context: Optional[SSLContext] = None

        self.__backend: str = 'local'
        self.__topologie: Optional[dict] = None
        self.__primaire: Optional[dict] = None

        self.__public_key_ssh_ed25519 = None
        self.__public_key_ssh_rsa = None

        # Semaphore pour empecher plusieurs jobs de s'executer en background en meme temps (DB lock)
        # self.__lock_db_job: Optional[asyncio.BoundedSemaphore] = None
        self.__sqlite_locks = sqlite_locks
        self.__sqlite_connection: Optional[sqlite3.Connection] = None

        # Declenche thread de sync backup secondaire
        self.__backup_event = asyncio.Event()

    async def ainit(self):
        self.__event_consignation_primaire_pret = asyncio.Event()
        await self.__sqlite_locks.ainit()

    async def reload_configuration(self):
        await super().reload_configuration()
        self.__ssl_context = SSLContext()
        self.__ssl_context.load_cert_chain(self.configuration.cert_pem_path, self.configuration.key_pem_path)

        # Charger les cles privees SSH
        path_cle_rsa = pathlib.Path(self.configuration.path_key_ssh_rsa)
        with open(path_cle_rsa, 'rb') as fichier:
            private_rsa = crypto_serialization.load_ssh_private_key(fichier.read(), password=None)
        path_cle_ed25519 = pathlib.Path(self.configuration.path_key_ssh_ed25519)
        with open(path_cle_ed25519, 'rb') as fichier:
            private_ed25519 = crypto_serialization.load_ssh_private_key(fichier.read(), password=None)

        # Exporter cles publiques
        self.__public_key_ssh_rsa = private_rsa.public_key().public_bytes(
            crypto_serialization.Encoding.OpenSSH,
            crypto_serialization.PublicFormat.OpenSSH
        ).decode('utf-8')
        self.__public_key_ssh_rsa += ' Fichiers %s' % self.clecertificat.enveloppe.subject_common_name
        self.__public_key_ssh_ed25519 = private_ed25519.public_key().public_bytes(
            crypto_serialization.Encoding.OpenSSH,
            crypto_serialization.PublicFormat.OpenSSH
        ).decode('utf-8')
        self.__public_key_ssh_ed25519 += ' Fichiers %s' % self.clecertificat.enveloppe.subject_common_name

    async def maj_topologie(self, configuration_topologie: dict):
        producer = self.producer
        if producer is None:
            await asyncio.sleep(2)  # Attendre connexion MQ
            producer = self.producer
            if producer is None:
                raise Exception('producer pas pret')
        await asyncio.wait_for(producer.producer_pret().wait(), 5)

        try:
            data_chiffre = configuration_topologie['data_chiffre']
            cle_id = data_chiffre.get('cle_id') or data_chiffre['ref_hachage_bytes']
        except (TypeError, KeyError, AttributeError):
            self.__logger.debug("maj_topologie Aucune configuration chiffree")
        else:
            try:
                requete_cle = {'cle_ids': [cle_id]}
                reponse_cle = await producer.executer_requete(
                    requete_cle, 'CoreTopologie', 'getCleConfiguration', exchange="2.prive")
                cle_dechiffree = reponse_cle.parsed['cles'].pop()
                cle_secrete: bytes = multibase.decode('m' + cle_dechiffree['cle_secrete_base64'])
                document_dechiffre = dechiffrer_document_secrete(cle_secrete, data_chiffre)
                configuration_topologie.update(document_dechiffre)
            except Exception :
                self.__logger.exception("maj_topologie Erreur dechiffrage configuration")
        self.topologie = configuration_topologie

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
        return self.__primaire

    @primaire.setter
    def primaire(self, primaire):
        self.__primaire = primaire

    @property
    def est_primaire(self):
        try:
            return self.primaire['instance_id'] == self.clecertificat.enveloppe.subject_common_name
        except (TypeError, KeyError):
            return None

    @property
    def url_consignation_primaire(self) -> Optional[str]:
        return self.__url_consignation_primaire

    # @property
    # def lock_db_job(self) -> asyncio.BoundedSemaphore:
    #     raise NotImplementedError('obsolete')
    #     # # Chargement valeurs au besoin
    #     # if self.__lock_db_job is None:
    #     #     self.__lock_db_job = asyncio.BoundedSemaphore(value=1)
    #     # return self.__lock_db_job

    def sqlite_connection(self, check_same_thread=False, readonly=False) -> SQLiteConnection:
        path_database = pathlib.Path(self.get_path_data(), Constantes.FICHIER_DATABASE_FICHIERS)

        # Creer une connexion reutilisable (qui ne se ferme pas apres chaque utilisation)
        return SQLiteConnection(
            path_database, self.__sqlite_locks, check_same_thread=check_same_thread, reuse=False, readonly=readonly)

    def get_path_data(self) -> pathlib.Path:
        return pathlib.Path(self.configuration.dir_data)

    def get_public_key_ssh(self) -> dict:
        return {'rsa': self.__public_key_ssh_rsa, 'ed25519': self.__public_key_ssh_ed25519}

    async def charger_consignation_primaire(self):
        producer = self.producer
        if producer is None:
            await asyncio.sleep(5)  # Attendre connexion MQ
            producer = self.producer
            if producer is None:
                raise Exception('producer pas pret')
        await asyncio.wait_for(producer.producer_pret().wait(), 30)

        reponse = await producer.executer_requete(
            {'primaire': True}, 'CoreTopologie', 'getConsignationFichiers', exchange="2.prive")

        try:
            consignation_url = reponse.parsed['consignation_url']
            self.__url_consignation_primaire = consignation_url
            self.__event_consignation_primaire_pret.set()
            return consignation_url
        except Exception as e:
            self.__logger.exception("Erreur chargement URL consignation")

    @property
    def consignation_primaire_pret(self):
        return self.__event_consignation_primaire_pret

    @property
    def sqlite_locks(self):
        return self.__sqlite_locks

    @property
    def backup_event(self):
        return self.__backup_event
