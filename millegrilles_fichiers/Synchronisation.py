import asyncio
import datetime
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

        self.__sync_event_primaire: Optional[asyncio.Event] = None
        self.__sync_event_secondaire: Optional[asyncio.Event] = None
        self.__reception_fuuids_reclames: Optional[asyncio.Queue] = None
        self.__attente_domaine_event: Optional[asyncio.Event] = None
        self.__attente_domaine_activite: Optional[datetime.datetime] = None

    def demarrer_sync_primaire(self):
        self.__sync_event_primaire.set()

    def demarrer_sync_secondaire(self):
        self.__sync_event_secondaire.set()

    async def run(self):
        self.__sync_event_primaire = asyncio.Event()
        self.__sync_event_secondaire = asyncio.Event()
        self.__reception_fuuids_reclames = asyncio.Queue(maxsize=3)
        await asyncio.gather(
            self.thread_sync_primaire(),
            self.thread_sync_secondaire(),
            self.thread_traiter_fuuids_reclames(),
        )

    async def thread_sync_primaire(self):
        pending = {self.__stop_event.wait()}
        while self.__stop_event.is_set() is False:
            pending.add(self.__sync_event_primaire.wait())
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            if self.__stop_event.is_set():
                break  # Done

            try:
                await self.run_sync_primaire()
            except Exception:
                self.__logger.exception("Erreur synchronisation")

            self.__sync_event_primaire.clear()

    async def thread_sync_secondaire(self):
        pending = {self.__stop_event.wait()}
        while self.__stop_event.is_set() is False:
            pending.add(self.__sync_event_secondaire.wait())
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            if self.__stop_event.is_set():
                break  # Done
            try:
                await self.run_sync_secondaire()
            except Exception:
                self.__logger.exception("Erreur synchronisation")

            self.__sync_event_secondaire.clear()

    async def thread_emettre_evenement_primaire(self, event_sync: asyncio.Event):
        wait_coro = event_sync.wait()
        while event_sync.is_set() is False:
            try:
                await self.emettre_etat_sync_primaire()
            except Exception as e:
                self.__logger.info("thread_emettre_evenement Erreur emettre etat sync : %s" % e)

            await asyncio.wait([wait_coro], timeout=5)

    async def thread_traiter_fuuids_reclames(self):
        stop_coro = self.__stop_event.wait()
        while self.__stop_event.is_set() is False:
            done, pending = await asyncio.wait([stop_coro, self.__reception_fuuids_reclames.get()], return_when=asyncio.FIRST_COMPLETED)
            if self.__stop_event.is_set() is True:
                break
            coro = done.pop()
            commande: dict = coro.result()
            if isinstance(commande, dict) is False:
                continue  # Mauvais type, skip

            termine = commande.get('termine') or False
            fuuids = commande.get('fuuids') or list()
            archive = commande.get('archive') or False

            if archive is True:
                bucket = Constantes.BUCKET_ARCHIVES
            else:
                bucket = Constantes.BUCKET_PRINCIPAL

            await self.__consignation.reclamer_fuuids_database(fuuids, bucket)

            if self.__attente_domaine_event is not None and termine:
                self.__attente_domaine_event.set()

    async def run_sync_primaire(self):
        self.__logger.info("thread_sync_primaire Demarrer sync")
        await self.emettre_etat_sync_primaire()

        event_sync = asyncio.Event()

        done, pending = await asyncio.wait(
            [
                self.thread_emettre_evenement_primaire(event_sync),
                self.__sequence_sync_primaire()
            ],
            return_when=asyncio.FIRST_COMPLETED
        )
        event_sync.set()  # Complete
        for t in pending:
            t.cancel('done')

        await self.emettre_etat_sync_primaire(termine=True)
        self.__logger.info("thread_sync_primaire Fin sync")

    async def __sequence_sync_primaire(self):
        # Date debut utilise pour trouver les fichiers orphelins (si reclamation est complete)
        debut_reclamation = datetime.datetime.utcnow()
        reclamation_complete = await self.reclamer_fuuids()

        # Process orphelins
        await self.__consignation.marquer_orphelins(debut_reclamation, reclamation_complete)

        # Generer la liste des reclamations en .jsonl.gz pour les secondaires
        await self.__consignation.generer_reclamations_sync()

    async def run_sync_secondaire(self):
        self.__logger.info("run_sync_secondaire Demarrer sync")
        await self.emettre_etat_sync_secondaire()

        event_sync = asyncio.Event()

        done, pending = await asyncio.wait(
            [
                self.thread_emettre_evenement_secondaire(event_sync),
                self.__sequence_sync_secondaire()
            ],
            return_when=asyncio.FIRST_COMPLETED
        )
        event_sync.set()  # Complete
        for t in pending:
            t.cancel('done')

        await self.emettre_etat_sync_secondaire(termine=True)
        self.__logger.info("run_sync_secondaire Fin sync")

    async def __sequence_sync_secondaire(self):
        # Download fichiers reclamations primaire
        # Merge information dans database
        await self.traiter_fichiers_reclamation()

        # Marquer orphelins

        # Determiner downloads

        # Determiner upload

        pass

    async def thread_emettre_evenement_secondaire(self, event_sync: asyncio.Event):
        wait_coro = event_sync.wait()
        while event_sync.is_set() is False:
            try:
                await self.emettre_etat_sync_secondaire()
            except Exception as e:
                self.__logger.info("thread_emettre_evenement_secondaire Erreur emettre etat sync : %s" % e)

            await asyncio.wait([wait_coro], timeout=5)

    async def reclamer_fuuids(self) -> bool:
        domaines = await self.get_domaines_reclamation()
        complet = True
        for domaine in domaines:
            resultat = await self.reclamer_fichiers_domaine(domaine)
            complet = complet and resultat

        return complet

    async def get_domaines_reclamation(self):
        producer = self.__etat_instance.producer
        await asyncio.wait_for(producer.producer_pret().wait(), timeout=5)

        requete = {'reclame_fuuids': True}
        reponse = await producer.executer_requete(
            requete,
            domaine=Constantes.DOMAINE_CORE_TOPOLOGIE, action=Constantes.REQUETE_LISTE_DOMAINES,
            exchange=ConstantesMillegrilles.SECURITE_PRIVE
        )

        resultats = reponse.parsed['resultats']
        domaines_reclamation = set()
        for domaine in resultats:
            domaines_reclamation.add(domaine['domaine'])

        return domaines_reclamation

    async def reclamer_fichiers_domaine(self, domaine: str):
        producer = self.__etat_instance.producer
        await asyncio.wait_for(producer.producer_pret().wait(), timeout=1)

        # S'assurer de liberer un event precedent
        if self.__attente_domaine_event is not None:
            self.__attente_domaine_event.set()

        self.__attente_domaine_event = asyncio.Event()

        requete = {}
        reponse = await producer.executer_commande(
            requete,
            domaine=domaine, action=Constantes.COMMANDE_RECLAMER_FUUIDS, exchange=ConstantesMillegrilles.SECURITE_PRIVE
        )

        if reponse.parsed['ok'] is not True:
            raise Exception('Erreur requete fichiers domaine %s' % domaine)

        # Attendre fin de reception
        wait_coro = self.__attente_domaine_event.wait()
        self.__attente_domaine_activite = datetime.datetime.utcnow()
        while self.__attente_domaine_event.is_set() is False:
            expire = datetime.datetime.utcnow() - datetime.timedelta(seconds=15)
            if expire > self.__attente_domaine_activite:
                # Timeout activite
                break
            await asyncio.wait([wait_coro], timeout=5)

        complete = self.__attente_domaine_event.is_set()
        self.__attente_domaine_event.set()
        self.__attente_domaine_event = None

        return complete

    async def emettre_etat_sync_primaire(self, termine=False):
        message = {'termine': termine}
        producer = self.__etat_instance.producer
        await asyncio.wait_for(producer.producer_pret().wait(), timeout=5)

        await producer.emettre_evenement(
            message,
            domaine=Constantes.DOMAINE_FICHIERS, action=Constantes.EVENEMENT_SYNC_PRIMAIRE,
            exchanges=ConstantesMillegrilles.SECURITE_PRIVE
        )

        if termine:
            # Emettre evenement pour declencher le sync secondaire
            self.__logger.debug("emettre_etat_sync Emettre evenement declencher sync secondaire")
            await producer.emettre_evenement(
                dict(),
                domaine=Constantes.DOMAINE_FICHIERS, action=Constantes.EVENEMENT_SYNC_SECONDAIRE,
                exchanges=ConstantesMillegrilles.SECURITE_PRIVE
            )

    async def emettre_etat_sync_secondaire(self, termine=False):
        message = {'termine': termine}
        producer = self.__etat_instance.producer
        await asyncio.wait_for(producer.producer_pret().wait(), timeout=5)

        # await producer.emettre_evenement(
        #     message,
        #     domaine=Constantes.DOMAINE_FICHIERS, action=Constantes.EVENEMENT_SYNC_PRIMAIRE,
        #     exchanges=ConstantesMillegrilles.SECURITE_PRIVE
        # )
        #
        # if termine:
        #     # Emettre evenement pour declencher le sync secondaire
        #     self.__logger.debug("emettre_etat_sync Emettre evenement declencher sync secondaire")
        #     await producer.emettre_evenement(
        #         dict(),
        #         domaine=Constantes.DOMAINE_FICHIERS, action=Constantes.EVENEMENT_SYNC_SECONDAIRE,
        #         exchanges=ConstantesMillegrilles.SECURITE_PRIVE
        #     )

    async def conserver_activite_fuuids(self, commande: dict):
        await self.__reception_fuuids_reclames.put(commande)

    async def traiter_fichiers_reclamation(self):
        """ Pour la sync secondaire. Download fichiers DB du primaire et merge avec DB locale. """
        pass
