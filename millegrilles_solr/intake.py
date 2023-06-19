# Intake de fichiers a indexer
import logging
import json
from typing import Optional

from asyncio import Event, TimeoutError, wait, FIRST_COMPLETED, gather

from millegrilles_messages.chiffrage.DechiffrageUtils import dechiffrer_document
from millegrilles_solr.EtatRelaiSolr import EtatRelaiSolr


class IntakeHandler:

    def __init__(self, stop_event: Event, etat_relai_solr: EtatRelaiSolr):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self._etat_relaisolr = etat_relai_solr
        self.__event_fichiers: Event = None
        self.__stop_event = stop_event

    async def configurer(self):
        self.__event_fichiers = Event()

    async def trigger_fichiers(self):
        self.__logger.info('IntakeHandler trigger fichiers recu')
        self.__event_fichiers.set()

    async def run(self):
        self.__logger.info('IntakeHandler running')
        await gather(self.traiter_fichiers())

    async def traiter_fichiers(self):
        while not self.__stop_event.is_set():
            try:
                if self.__event_fichiers.is_set() is False:
                    await wait(
                        [self.__stop_event.wait(), self.__event_fichiers.wait()],
                        timeout=20, return_when=FIRST_COMPLETED
                    )
            except TimeoutError:
                self.__logger.debug("Verifier si fichier disponible pour indexation")
                self.__event_fichiers.set()
            else:
                if self.__stop_event.is_set():
                    self.__logger.info('Arret loop traiter_fichiers')
                    break

            try:
                # Requete prochain fichier
                job = await self.get_prochain_fichier()

                if job is not None:
                    # Downloader/dechiffrer
                    fuuid = job['fuuid']
                    mimetype = job['mimetype']
                    if mimetype_supporte_fulltext(mimetype):
                        self.__logger.debug("Downloader %s" % fuuid)

                    info_fichier = await self.dechiffrer_metadata(job)

                    self.__logger.debug("Indexer fichier %s" % json.dumps(info_fichier, indent=2))

                    # Indexer
                    pass

                else:
                    self.__event_fichiers.clear()
            except Exception as e:
                self.__logger.error("traiter_fichiers Erreur traitement : %s" % e)
                # Erreur generique non geree. Creer un delai de traitement pour poursuivre
                self.__event_fichiers.clear()

    async def get_prochain_fichier(self) -> Optional[dict]:

        try:
            producer = self._etat_relaisolr.producer
            job_indexation = await producer.executer_commande(
                dict(), 'GrosFichiers', 'getJobIndexation', exchange="4.secure")
            if job_indexation.parsed['ok'] == True:
                self.__logger.debug("Executer job indexation : %s" % job_indexation)
                return job_indexation.parsed
            else:
                self.__logger.debug("Aucune job d'indexation disponible")
        except Exception as e:
            self.__logger.error("Erreur recuperation job indexation : %s" % e)

        return None

    async def dechiffrer_metadata(self, job):
        cle = job['cle']['cle']
        metadata = job['metadata']
        clecert = self._etat_relaisolr.clecertificat
        doc_dechiffre = dechiffrer_document(clecert, cle, metadata)
        return doc_dechiffre


MIMETYPES_FULLTEXT = ['application/pdf']
BASE_FULLTEXT = ['text']


def mimetype_supporte_fulltext(mimetype) -> bool:

    if mimetype in MIMETYPES_FULLTEXT:
        return True
    else:
        prefix = mimetype.split('/')[0]
        if prefix in BASE_FULLTEXT:
            return True

    return False
