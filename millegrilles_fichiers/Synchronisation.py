import asyncio
import logging

from typing import Optional

from millegrilles_messages.messages import Constantes as ConstantesMillegrilles

from millegrilles_fichiers import Constantes
# from millegrilles_fichiers.Consignation import ConsignationHandler
from millegrilles_fichiers.EtatFichiers import EtatFichiers


class SyncManager:

    def __init__(self, consignation):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__consignation = consignation

        self.__stop_event = consignation.stop_event
        self.__etat_instance: EtatFichiers = consignation.etat_instance

        self.__sync_event: Optional[asyncio.Event] = None

    def demarrer_sync(self):
        self.__sync_event.set()

    async def run(self):
        self.__sync_event = asyncio.Event()
        await asyncio.gather(
            self.thread_sync_primaire(),
        )

    async def thread_sync_primaire(self):
        pending = {self.__stop_event.wait()}
        while self.__stop_event.is_set() is False:
            pending.add(self.__sync_event.wait())
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

            try:
                await self.run_sync()
            except Exception:
                self.__logger.exception("Erreur synchronisation")

            self.__sync_event.clear()

    async def thread_emettre_evenement(self, event_sync: asyncio.Event):
        wait_coro = event_sync.wait()
        while event_sync.is_set() is False:
            try:
                await self.emettre_etat_sync()
            except Exception as e:
                self.__logger.info("thread_emettre_evenement Erreur emettre etat sync : %s" % e)

            await asyncio.wait([wait_coro], timeout=5)

    async def run_sync(self):
        self.__logger.info("thread_sync_primaire Demarrer sync")
        await self.emettre_etat_sync()

        event_sync = asyncio.Event()

        done, pending = await asyncio.wait(
            [
                self.thread_emettre_evenement(event_sync),
                self.__sequence_sync()
            ],
            return_when=asyncio.FIRST_COMPLETED
        )
        event_sync.set()  # Complete
        for t in pending:
            t.cancel('done')

        await self.emettre_etat_sync(termine=True)
        self.__logger.info("thread_sync_primaire Fin sync")

    async def __sequence_sync(self):
        await self.reclamer_fuuids()

    async def reclamer_fuuids(self):
        pass

    async def emettre_etat_sync(self, termine=False):
        message = {'termine': termine}
        producer = self.__etat_instance.producer
        await asyncio.wait_for(producer.producer_pret().wait(), timeout=5)

        await producer.emettre_evenement(
            message,
            domaine=Constantes.DOMAINE_FICHIERS, action=Constantes.EVENEMENT_SYNC_PRIMAIRE,
            exchanges=ConstantesMillegrilles.SECURITE_PRIVE
        )
