import asyncio
import logging

from typing import Optional

from ssl import SSLContext

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

    async def reload_configuration(self):
        await super().reload_configuration()
        self.__ssl_context = SSLContext()
        self.__ssl_context.load_cert_chain(self.configuration.cert_pem_path, self.configuration.key_pem_path)

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
        return self.primaire

    @primaire.setter
    def primaire(self, primaire):
        self.__primaire = primaire
