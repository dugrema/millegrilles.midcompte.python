import asyncio
import logging
import pathlib

from typing import Optional

from ssl import SSLContext

from cryptography.hazmat.primitives import serialization as crypto_serialization

from millegrilles_messages.chiffrage.DechiffrageUtils import dechiffrer_document
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

        self.__public_key_ssh_ed25519 = None
        self.__public_key_ssh_rsa = None

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
            ref_hachage_bytes = data_chiffre['ref_hachage_bytes']
            requete_cle = {'ref_hachage_bytes': ref_hachage_bytes}
            reponse_cle = await producer.executer_requete(
                requete_cle, 'CoreTopologie', 'getCleConfiguration', exchange="2.prive")
            cle_secrete = reponse_cle.parsed['cles'][ref_hachage_bytes]['cle']
            document_dechiffre = dechiffrer_document(self.clecertificat, cle_secrete, data_chiffre)
            configuration_topologie.update(document_dechiffre)
        except (TypeError, KeyError) as e:
            self.__logger.debug("Aucune configuration chiffree ou erreur dechiffrage : %s" % e)
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

    def get_public_key_ssh(self) -> dict:
        return {'rsa': self.__public_key_ssh_rsa, 'ed25519': self.__public_key_ssh_ed25519}
