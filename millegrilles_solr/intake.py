# Intake de fichiers a indexer
import logging
from asyncio import Event, TimeoutError, wait, FIRST_COMPLETED, gather

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
        self.__event_fichiers.set()

    async def run(self):
        self.__logger.info('IntakeHandler running')
        await gather(self.traiter_fichiers())

    async def traiter_fichiers(self):
        while True:
            try:
                if self.__event_fichiers.is_set() is False:
                    await wait(
                        [self.__stop_event.wait(), self.__event_fichiers.wait()],
                        timeout=60, return_when=FIRST_COMPLETED
                    )
            except TimeoutError:
                self.__event_fichiers.set()
            else:
                if self.__stop_event.is_set():
                    self.__logger.info('Arret loop traiter_fichiers')
                    break

            try:
                # Requete prochain fichier
                raise NotImplementedError('todo')

                # Downloader/dechiffrer

                # Indexer
                pass
            except Exception as e:
                self.__logger.error("traiter_fichiers Erreur traitement : %s" % e)

            self.__event_fichiers.clear()  # TODO : clear juste si fichier non disponible
