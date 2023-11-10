import tempfile

import aiohttp
import asyncio
import datetime
import errno
import gzip
import json
import logging
import pathlib
import pytz

from cryptography.exceptions import InvalidSignature
from certvalidator.errors import PathValidationError
from typing import Optional
from urllib3.util import parse_url
from aiohttp.client_exceptions import ClientResponseError

from millegrilles_messages.messages import Constantes as ConstantesMillegrilles

from millegrilles_fichiers import Constantes
# from millegrilles_fichiers.Consignation import ConsignationHandler
from millegrilles_fichiers.EtatFichiers import EtatFichiers
from millegrilles_fichiers.SQLiteDao import SQLiteConnection
from millegrilles_fichiers.UploadFichiersPrimaire import uploader_fichier, EtatUpload
from millegrilles_fichiers.SQLiteDao import (SQLiteConnection, SQLiteReadOperations, SQLiteWriteOperations,
                                             SQLiteBatchOperations,
                                             SQLiteDetachedSyncCreate, SQLiteDetachedSyncApply,
                                             SQLiteDetachedReclamationAppend, SQLiteDetachedBackupAppend,
                                             SQLiteDetachedTransferApply, SQLiteTransfertOperations,
                                             SQLiteDetachedReclamationFichierAppend, SQLiteTransfertOperations)

CONST_LIMITE_SAMPLES_DOWNLOAD = 50  # Utilise pour calcul taux de transfert


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

        self.__upload_event: Optional[asyncio.Event] = None
        self.__download_event: Optional[asyncio.Event] = None
        self.__backup_event: Optional[asyncio.Event] = None
        # self.__event_attendre_visite: Optional[asyncio.Event] = None

        self.__nombre_fuuids_reclames_domaine = 0
        self.__total_fuuids_reclames_domaine: Optional[int] = None

        self.__download_en_cours: Optional[dict] = None
        self.__samples_download = list()  # Utilise pour calcul de vitesse
        self.__upload_en_cours: Optional[EtatUpload] = None
        self.__samples_upload = list()  # Utilise pour calcul de vitesse

    @property
    def sync_en_cours(self) -> bool:
        return self.__sync_event_primaire.is_set() or self.__sync_event_secondaire.is_set()

    def get_path_database_sync(self) -> pathlib.Path:
        return pathlib.Path(self.__etat_instance.get_path_data(), Constantes.FICHIER_DATABASE_SYNC)

    def demarrer_sync_primaire(self):
        self.__sync_event_primaire.set()

    def demarrer_sync_secondaire(self):
        self.__sync_event_secondaire.set()

    # def set_visite_completee(self):
    #     """ Appele lorsque la premiere visite de tous les fichiers a ete completee """
    #     self.__event_attendre_visite.set()

    async def run(self):
        self.__sync_event_primaire = asyncio.Event()
        self.__sync_event_secondaire = asyncio.Event()
        self.__reception_fuuids_reclames = asyncio.Queue(maxsize=3)
        self.__upload_event = asyncio.Event()
        self.__download_event = asyncio.Event()
        self.__backup_event = asyncio.Event()
        # self.__event_attendre_visite = asyncio.Event()

        await asyncio.gather(
            self.thread_sync_primaire(),
            self.thread_sync_secondaire(),
            self.thread_traiter_fuuids_reclames(),
            self.thread_upload(),
            self.thread_download(),
            self.thread_entretien_transferts(),
            # self.thread_sync_backup(),
        )

    async def thread_sync_primaire(self):
        # pending = {asyncio.create_task(self.__stop_event.wait()), asyncio.create_task(self.__event_attendre_visite.wait())}
        # self.__logger.info('thread_sync_primaire Attendre premiere visite complete des fuuids')
        # done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

        # for d in done:
        #     if d.exception():
        #         raise d.exception()
        #
        # if self.__stop_event.is_set():
        #     return  # Stopping
        # self.__logger.info('thread_sync_primaire Deblocage thread apres premiere visite complete des fuuids')

        pending = {asyncio.create_task(self.__stop_event.wait())}
        while self.__stop_event.is_set() is False:
            pending.add(asyncio.create_task(self.__sync_event_primaire.wait()))
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

            for d in done:
                if d.exception():
                    self.__logger.debug("thread_sync_primaire Erreur exceution thread : %s" % d.exception())

            if self.__stop_event.is_set():
                for p in pending:
                    p.cancel()
                    try:
                        await p
                    except asyncio.CancelledError:
                        pass  # OK
                break  # Done

            if len(pending) == 0:
                raise Exception('arrete indirectement (stop event gone)')

            try:
                await self.run_sync_primaire()
            except Exception:
                self.__logger.exception("Erreur synchronisation")

            self.__sync_event_primaire.clear()

    async def thread_sync_secondaire(self):
        # pending = {self.__stop_event.wait(), self.__event_attendre_visite.wait()}
        # self.__logger.info('thread_sync_secondaire Attendre premiere visite complete des fuuids')
        # done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        # if self.__stop_event.is_set():
        #     return  # Stopping
        # self.__logger.info('thread_sync_secondaire Deblocage thread apres premiere visite complete des fuuids')

        pending = {asyncio.create_task(self.__stop_event.wait())}
        while self.__stop_event.is_set() is False:
            pending.add(asyncio.create_task(self.__sync_event_secondaire.wait()))
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            if self.__stop_event.is_set():
                break  # Done
            try:
                await self.run_sync_secondaire()
            except Exception:
                self.__logger.exception("Erreur synchronisation")

            self.__sync_event_secondaire.clear()

            if len(pending) == 0:
                raise Exception('arrete indirectement (stop event gone)')

    async def thread_upload(self):
        pending = {asyncio.create_task(self.__stop_event.wait())}
        while self.__stop_event.is_set() is False:
            pending.add(asyncio.create_task(self.__upload_event.wait()))
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            if self.__stop_event.is_set():
                break  # Done
            try:
                await self.run_upload()
            except Exception:
                self.__logger.exception("thread_upload Erreur synchronisation")

            self.__upload_event.clear()

            if len(pending) == 0:
                raise Exception('arrete indirectement (stop event gone)')

        # Terminer execution de toutes les tasks
        for p in pending:
            try:
                p.cancel()
            except AttributeError:
                pass
        await asyncio.wait(pending, timeout=1)

    async def thread_download(self):
        pending = {asyncio.create_task(self.__stop_event.wait())}
        while self.__stop_event.is_set() is False:
            pending.add(asyncio.create_task(self.__download_event.wait()))
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            if self.__stop_event.is_set():
                break  # Done
            try:
                await self.run_download()
            except Exception:
                self.__logger.exception("thread_download Erreur synchronisation")

            self.__download_event.clear()

            if len(pending) == 0:
                raise Exception('arrete indirectement (stop event gone)')

        # Terminer execution de toutes les tasks
        for p in pending:
            try:
                p.cancel()
            except AttributeError:
                pass
        await asyncio.wait(pending, timeout=1)

    async def thread_entretien_transferts(self):
        while self.__stop_event.is_set() is False:
            try:
                await self.run_entretien_transferts()
            except Exception:
                self.__logger.exception("thread_download Erreur synchronisation")
            try:
                await asyncio.wait_for(self.__stop_event.wait(), timeout=900)
            except asyncio.TimeoutError:
                pass  # OK

    async def thread_emettre_evenement_primaire(self, event_sync: asyncio.Event):
        while event_sync.is_set() is False:
            try:
                await self.emettre_etat_sync_primaire()
            except Exception as e:
                self.__logger.info("thread_emettre_evenement Erreur emettre etat sync : %s" % e)

            try:
                await asyncio.wait_for(event_sync.wait(), timeout=5)
            except asyncio.TimeoutError:
                pass  # OK

    async def thread_traiter_fuuids_reclames(self):
        pending = {self.__stop_event.wait()}
        while self.__stop_event.is_set() is False:
            # Ajouter get queue (async block)
            pending.add(asyncio.create_task(self.__reception_fuuids_reclames.get()))

            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            if self.__stop_event.is_set() is True:
                break

            for d in done:
                if d.exception():
                    self.__logger.error("thread_traiter_fuuids_reclames Erreur traitement : %s" % d.exception())
                else:
                    commande: Optional[dict] = d.result()

                    while commande is not None:
                        if not commande or isinstance(commande, dict) is False:
                            commande = None
                            continue  # Mauvais type, skip

                        termine = commande.get('termine') or False
                        fuuids = commande.get('fuuids') or list()
                        archive = commande.get('archive') or False
                        total = commande.get('total')

                        self.__nombre_fuuids_reclames_domaine += len(fuuids)

                        if total:
                            self.__total_fuuids_reclames_domaine = total

                        if archive is True:
                            bucket = Constantes.BUCKET_ARCHIVES
                        else:
                            bucket = Constantes.BUCKET_PRINCIPAL

                        # Faire un touch d'activite avant et apres traitement pour eviter un timeout
                        self.__attente_domaine_activite = datetime.datetime.utcnow()
                        await self.__consignation.reclamer_fuuids_database(fuuids, bucket)
                        # await asyncio.sleep(0.5)  # Throttle pour permettre acces DB
                        self.__attente_domaine_activite = datetime.datetime.utcnow()

                        if self.__attente_domaine_event is not None and termine:
                            self.__attente_domaine_event.set()

                        try:
                            commande = self.__reception_fuuids_reclames.get_nowait()
                        except asyncio.QueueEmpty:
                            # Done
                            commande = None

            if len(pending) == 0:
                raise Exception('arrete indirectement (stop event gone)')

        # Terminer execution de toutes les tasks
        for p in pending:
            try:
                p.cancel()
            except AttributeError:
                pass
        await asyncio.wait(pending, timeout=1)

    async def run_sync_primaire(self):
        self.__logger.info("thread_sync_primaire Demarrer sync")
        try:
            await self.emettre_etat_sync_primaire()
        except asyncio.TimeoutError:
            self.__logger.info("thread_sync_primaire Erreur emission etat sync initial (timeout)")

        event_sync = asyncio.Event()
        tasks = [
            self.thread_emettre_evenement_primaire(event_sync),
            self.__sequence_sync_primaire(event_sync)
        ]
        await asyncio.gather(*tasks)

        try:
            await self.emettre_etat_sync_primaire(termine=True)
        except asyncio.TimeoutError:
            self.__logger.info("thread_sync_primaire Erreur emission etat sync final (timeout)")

        self.__logger.info("thread_sync_primaire Fin sync")

    async def __sequence_sync_primaire(self, event_sync: asyncio.Event):
        try:
            path_database_sync = self.get_path_database_sync()

            # Supprimer base de donnees de sync
            path_database_sync.unlink(missing_ok=True)

            with SQLiteConnection(self.get_path_database_sync(), None, check_same_thread=False, reuse=False) as connection:
                # Date debut utilise pour trouver les fichiers orphelins (si reclamation est complete)
                debut_reclamation = datetime.datetime.utcnow()

                # Initialiser la base de donnees de synchronisation
                async with SQLiteDetachedSyncCreate(connection):
                    # L'ouverture execute la creation de la db
                    self.__logger.debug("Nouvelle base de donnes de sync cree (%s)" % connection.path_database)

                self.__logger.info("__sequence_sync_primaire Debut reclamation (Progres 1/5)")
                reclamation_complete = await self.reclamer_fuuids()
                self.__logger.info("__sequence_sync_primaire Debut fin reclamation (complet? %s)" % reclamation_complete)

                # Debloquer les visites (pour prochaine visite)
                self.__consignation.timestamp_visite = datetime.datetime.utcnow()
                # Ajouter visiter_fuuids dans les taches de sync
                # tasks_initiales.append(asyncio.create_task(self.__consignation.visiter_fuuids(dao_batch)))
                self.__logger.info("__sequence_sync_primaire visiter_fuuids (Progres: 2/5)")
                await self.__consignation.visiter_fuuids(connection)

                #debut_reclamation = datetime.datetime.utcnow()
                #resultat_initial = await asyncio.gather(*tasks_initiales)
                #reclamation_complete = resultat_initial[0]

                if self.__stop_event.is_set():
                    return  # Stopped

                with self.__etat_instance.sqlite_connection() as connection_fichiers:
                    path_database_fichiers = connection_fichiers.path_database

                # Transferer contenu de la base de donnes sync.sqlite version consignation.sqlite
                async with SQLiteDetachedSyncApply(connection, debut_reclamation) as sync_dao:
                    self.__logger.debug("__sequence_sync_primaire Sync vers base de donnee de fichiers")

                    # Attacher la database de fichiers (destination)
                    await sync_dao.attach_destination(path_database_fichiers, 'fichiers')

                    pass  # Fermer, le transfert s'execute automatiquement a la fermeture

            if self.__stop_event.is_set():
                return  # Stopped

            with self.__etat_instance.sqlite_connection(check_same_thread=False) as connection:
                # Generer la liste des reclamations en .jsonl.gz pour les secondaires
                self.__logger.info("__sequence_sync_primaire generer_reclamations_sync (Progres: 4/5)")
                await self.__consignation.generer_reclamations_sync(connection)

            if self.__stop_event.is_set():
                return  # Stopped

            # Generer la liste des fichiers de backup (aucun acces db requis)
            self.__logger.info("__sequence_sync_primaire generer_backup_sync (Progres: 5/5)")
            await self.__consignation.generer_backup_sync()

        finally:
            self.__logger.info("__sequence_sync_primaire termine")
            event_sync.set()

    async def run_sync_secondaire(self):
        self.__logger.info("run_sync_secondaire Demarrer sync")
        try:
            await self.emettre_etat_sync_secondaire()
        except asyncio.TimeoutError:
            self.__logger.info("thread_sync_secondaire Erreur emission etat sync initial (timeout)")

        event_sync = asyncio.Event()

        tasks = [
            self.thread_emettre_evenement_secondaire(event_sync),
            self.__sequence_sync_secondaire(event_sync)
        ]
        await asyncio.gather(*tasks)

        try:
            await self.emettre_etat_sync_secondaire(termine=True)
        except asyncio.TimeoutError:
            self.__logger.info("thread_sync_primaire Erreur emission etat sync final (timeout)")

        self.__logger.info("run_sync_secondaire Fin sync")

    async def __sequence_sync_secondaire(self, event_sync: asyncio.Event):
        path_database_sync = self.get_path_database_sync()

        # Supprimer base de donnees de sync
        path_database_sync.unlink(missing_ok=True)

        path_database_fichiers = self.__etat_instance.sqlite_connection().path_database
        path_database_transferts = pathlib.Path(self.__etat_instance.get_path_data(),
                                                Constantes.FICHIER_DATABASE_TRANSFERTS)
        with SQLiteConnection(path_database_transferts, None, check_same_thread=False, reuse=False) as connection:
            async with SQLiteTransfertOperations(connection) as transfert_dao:
                await transfert_dao.init_database()

        try:
            with SQLiteConnection(path_database_sync, None, check_same_thread=False, reuse=False) as connection:
                # Initialiser la base de donnees de synchronisation
                async with SQLiteDetachedSyncCreate(connection):
                    # L'ouverture execute la creation de la db
                    self.__logger.debug("__sequence_sync_secondaire Nouvelle base de donnees de sync cree (%s)" % connection.path_database)

                # Visites
                self.__consignation.timestamp_visite = datetime.datetime.utcnow()
                self.__logger.info("__sequence_sync_secondaire visiter_fuuids (Progres: 1/5)")
                await self.__consignation.visiter_fuuids(connection)

                # Reclamations (download du primaire)
                self.__logger.info(
                    "__sequence_sync_secondaire download_fichiers_reclamation (Progres: 2/5)")
                try:
                    await self.download_fichiers_reclamation()
                except aiohttp.client.ClientResponseError as e:
                    if e.status == 404:
                        self.__logger.error("__sequence_sync_secondaire Fichier de reclamation primaire n'est pas disponible (404)")
                    else:
                        self.__logger.error(
                            "__sequence_sync_secondaire Fichier de reclamation primaire non accessible (%d)" % e.status)
                    return  # Abandonner la sync

                if self.__stop_event.is_set():
                    return  # Stopped

                # Merge information reclamations dans database
                self.__logger.info("__sequence_sync_secondaire merge_fichiers_reclamation (Progres: 3/5)")
                debut_reclamation = datetime.datetime.now(tz=pytz.UTC)
                await self.merge_fichiers_reclamation(connection)

                if self.__stop_event.is_set():
                    return  # Stopped

                # Transferer data vers consignation.sqlite
                self.__logger.info("__sequence_sync_secondaire merge reclamations+visites avec main db (Progres: 4/5)")
                async with SQLiteDetachedSyncApply(connection, debut_reclamation) as sync_dao:
                    self.__logger.debug("__sequence_sync_primaire Sync vers base de donnee de fichiers")
                    await sync_dao.attach_destination(path_database_fichiers, 'fichiers')
                    pass  # Fermer, la sync s'execute automatiquement

                # Initialiser la base de donnees transfert.sqlite
                self.__logger.info("__sequence_sync_secondaire Creer operations de transfert (Progres: 5/5)")
                async with SQLiteDetachedTransferApply(connection) as transfert_dao:
                    try:
                        await transfert_dao.attach_destination(path_database_transferts, 'transferts')
                        pass  # Fermer, la sync s'execute automatiquement
                    except Exception as e:
                        self.__logger.exception("__sequence_sync_secondaire Erreur preparation SQLiteDetachedTransferApply")
                        raise e

                # Declencher les threads d'upload et de download (aucun effect si threads deja actives)
                self.__upload_event.set()
                self.__download_event.set()

                # Declencher sync des fichiers de backup avec le primaire
                self.__logger.info("__sequence_sync_secondaire run_sync_backup (Progres: 5/5)")
                await self.run_sync_backup()

        finally:
            self.__logger.info("__sequence_sync_primaire termine")
            event_sync.set()

        # try:
        #     with self.__etat_instance.sqlite_connection() as connection:
        #         async with SQLiteBatchOperations(connection) as dao_batch:
        #
        #             # tasks_initiales = [
        #             #     self.download_fichiers_reclamation(),
        #             # ]
        #
        #             # Download fichiers reclamations primaire
        #             if self.__consignation.timestamp_visite is None:
        #                 # Debloquer les visites (pour prochaine visite)
        #                 self.__consignation.timestamp_visite = datetime.datetime.utcnow()
        #                 # Ajouter visiter_fuuids dans les taches de sync
        #                 # tasks_initiales.append(self.__consignation.visiter_fuuids(dao_batch))
        #                 self.__logger.info("__sequence_sync_secondaire visiter_fuuids (Progres: 1/5)")
        #                 await self.__consignation.visiter_fuuids(dao_batch)
        #             else:
        #                 self.__logger.info("__sequence_sync_secondaire skip visiter fuuids (Progres: 1/5)")
        #
        #             self.__logger.info(
        #                 "__sequence_sync_secondaire download_fichiers_reclamation (Progres: 2/5)")
        #             try:
        #                 await self.download_fichiers_reclamation()
        #             except aiohttp.client.ClientResponseError as e:
        #                 if e.status == 404:
        #                     self.__logger.error("__sequence_sync_secondaire Fichier de reclamation primaire n'est pas disponible (404)")
        #                 else:
        #                     self.__logger.error(
        #                         "__sequence_sync_secondaire Fichier de reclamation primaire non accessible (%d)" % e.status)
        #                 return  # Abandonner la sync
        #
        #
        #             # try:
        #             #     await asyncio.gather(*tasks_initiales)
        #             # except aiohttp.client.ClientResponseError as e:
        #             #     if e.status == 404:
        #             #         self.__logger.error("__sequence_sync_secondaire Fichier de reclamation primaire n'est pas disponible (404)")
        #             #     else:
        #             #         self.__logger.error(
        #             #             "__sequence_sync_secondaire Fichier de reclamation primaire non accessible (%d)" % e.status)
        #             #     return  # Abandonner la sync
        #
        #     # try:
        #     #     self.__logger.info("__sequence_sync_secondaire download_fichiers_reclamation (Progres: 1/4)")
        #     #     await self.download_fichiers_reclamation()
        #     # except aiohttp.client.ClientResponseError as e:
        #     #     if e.status == 404:
        #     #         self.__logger.error("__sequence_sync_secondaire Fichier de reclamation primaire n'est pas disponible (404)")
        #     #     else:
        #     #         self.__logger.error(
        #     #             "__sequence_sync_secondaire Fichier de reclamation primaire non accessible (%d)" % e.status)
        #     #     return  # Abandonner la sync
        #
        #     if self.__stop_event.is_set():
        #         return  # Stopped
        #
        #     # Merge information dans database
        #     self.__logger.info("__sequence_sync_secondaire merge_fichiers_reclamation (Progres: 3/5)")
        #     await self.merge_fichiers_reclamation()
        #
        #     if self.__stop_event.is_set():
        #         return  # Stopped
        #
        #     # Ajouter manquants, marquer fichiers reclames
        #     # Marquer orphelins, determiner downloads et upload
        #     self.__logger.info("__sequence_sync_secondaire creer_operations_sur_secondaire (Progres: 4/5)")
        #     await self.creer_operations_sur_secondaire()
        #
        #     if self.__stop_event.is_set():
        #         return  # Stopped
        #
        #     # Declencher sync des fichiers de backup avec le primaire
        #     self.__logger.info("__sequence_sync_secondaire run_sync_backup (Progres: 5/5)")
        #     await self.run_sync_backup()
        # finally:
        #     self.__logger.info("__sequence_sync_secondaire Termine")
        #     event_sync.set()

    async def thread_emettre_evenement_secondaire(self, event_sync: asyncio.Event):
        while event_sync.is_set() is False:
            try:
                await self.emettre_etat_sync_secondaire()
            except Exception as e:
                self.__logger.info("thread_emettre_evenement_secondaire Erreur emettre etat sync : %s" % e)

            try:
                await asyncio.wait_for(event_sync.wait(), timeout=5)
            except asyncio.TimeoutError:
                pass  # OK

    async def reclamer_fuuids(self) -> bool:
        domaines = await self.get_domaines_reclamation()
        complet = True
        self.__nombre_fuuids_reclames_domaine = 0  # Compteur de fuuids recus
        self.__total_fuuids_reclames_domaine = None
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
        try:
            reponse = await producer.executer_commande(
                requete,
                domaine=domaine, action=Constantes.COMMANDE_RECLAMER_FUUIDS, exchange=ConstantesMillegrilles.SECURITE_PRIVE
            )
        except asyncio.TimeoutError:
            self.__logger.warning("reclamer_fichiers_domaine Timeout sur domaine %s, pas de reclamations recues (incomplet)" % domaine)
            return False

        if reponse.parsed['ok'] is not True:
            raise Exception('Erreur requete fichiers domaine %s' % domaine)

        # Attendre fin de reception
        self.__attente_domaine_activite = datetime.datetime.utcnow()
        while self.__attente_domaine_event.is_set() is False:
            expire = datetime.datetime.utcnow() - datetime.timedelta(seconds=20)
            if expire > self.__attente_domaine_activite:
                # Timeout activite
                break
            try:
                await asyncio.wait_for(self.__attente_domaine_event.wait(), timeout=5)
            except asyncio.TimeoutError:
                pass  # OK

        complete = (self.__attente_domaine_event.is_set() and
                    self.__nombre_fuuids_reclames_domaine == self.__total_fuuids_reclames_domaine)

        # Reset event, compteurs pour prochain domaine
        self.__attente_domaine_event.set()
        self.__attente_domaine_event = None
        self.__nombre_fuuids_reclames_domaine = 0
        self.__total_fuuids_reclames_domaine = 0

        return complete

    async def emettre_etat_sync_primaire(self, termine=False):
        message = {'termine': termine}
        producer = self.__etat_instance.producer
        await asyncio.wait_for(producer.producer_pret().wait(), timeout=1)

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
        pass  # Rien a faire, pas d'etat utilise pour secondaire

    async def conserver_activite_fuuids(self, commande: dict):
        await self.__reception_fuuids_reclames.put(commande)

    async def download_fichiers_reclamation(self):
        """ Pour la sync secondaire. Download fichiers DB du primaire et merge avec DB locale. """
        # Download fichiers reclamation
        url_consignation_primaire = self.__etat_instance.url_consignation_primaire

        url_primaire = parse_url(url_consignation_primaire)
        url_primaire_reclamations = parse_url(
            '%s/%s/%s' % (url_primaire.url, 'fichiers_transfert/sync', Constantes.FICHIER_RECLAMATIONS_PRIMAIRES))
        url_primaire_reclamations_intermediaires = parse_url(
            '%s/%s/%s' % (url_primaire.url, 'fichiers_transfert/sync', Constantes.FICHIER_RECLAMATIONS_INTERMEDIAIRES))
        url_primaire_backup = parse_url(
            '%s/%s/%s' % (url_primaire.url, 'fichiers_transfert/sync', Constantes.FICHIER_BACKUP))

        path_data = pathlib.Path(self.__etat_instance.configuration.dir_consignation, Constantes.DIR_DATA)
        path_reclamations = pathlib.Path(path_data, Constantes.FICHIER_RECLAMATIONS_PRIMAIRES)
        path_reclamations_work = pathlib.Path('%s.work' % path_reclamations)
        path_reclamations_intermediaire = pathlib.Path(path_data, Constantes.FICHIER_RECLAMATIONS_INTERMEDIAIRES)
        path_reclamations_intermediaire_work = pathlib.Path('%s.work' % path_reclamations_intermediaire)
        path_backup = pathlib.Path(path_data, Constantes.FICHIER_BACKUP)
        path_backup_work = pathlib.Path('%s.work' % path_backup)

        timeout = aiohttp.ClientTimeout(connect=20, total=600)
        async with aiohttp.ClientSession(timeout=timeout) as session:

            # Download reclamation primaires - doit etre present. Erreur 404 indique un echec et on ne poursuit pas.
            with path_reclamations_work.open(mode='wb') as output_file:
                self.__logger.info("traiter_fichiers_reclamation Downloader fichier %s" % url_primaire_reclamations.url)
                async with session.get(url_primaire_reclamations.url, ssl=self.__etat_instance.ssl_context) as resp:
                    resp.raise_for_status()  # Arreter sur toute erreur
                    async for chunk in resp.content.iter_chunked(64 * 1024):
                        output_file.write(chunk)

            # Renommer le fichier .work
            path_reclamations.unlink(missing_ok=True)
            path_reclamations_work.rename(path_reclamations)

            # Download reclamations intermediaires. Erreur 404 indique qu'on n'a pas de changement depuis creation
            # du fichier primaire (OK).
            fichier_intermediare_disponible = False
            with path_reclamations_intermediaire_work.open(mode='wb') as output_file:
                self.__logger.info("traiter_fichiers_reclamation Downloader fichier %s" % url_primaire_reclamations_intermediaires.url)
                async with session.get(url_primaire_reclamations_intermediaires.url, ssl=self.__etat_instance.ssl_context) as resp:
                    if resp.status == 404:
                        self.__logger.debug("traiter_fichiers_reclamation Fichier intermediaire non disponible (404) - OK")
                    elif resp.status != 200:
                        self.__logger.debug("traiter_fichiers_reclamation Erreur acces au fichier intermediaire (%d) - on ignore le fichier" % resp.status)
                    else:
                        fichier_intermediare_disponible = True
                        async for chunk in resp.content.iter_chunked(64 * 1024):
                            output_file.write(chunk)

            # Renommer le fichier .work si present
            path_reclamations_intermediaire.unlink(missing_ok=True)
            if fichier_intermediare_disponible:
                path_reclamations_intermediaire_work.rename(path_reclamations_intermediaire)
            else:
                # Retirer le fichier vide
                path_reclamations_intermediaire_work.unlink()

            fichier_backup_disponible = False
            with path_backup_work.open(mode='wb') as output_file:
                self.__logger.info(
                    "traiter_fichiers_reclamation Downloader fichier %s" % url_primaire_backup.url)
                async with session.get(url_primaire_backup.url, ssl=self.__etat_instance.ssl_context) as resp:
                    if resp.status == 404:
                        self.__logger.debug("traiter_fichiers_reclamation Fichier backup non disponible (404) - OK")
                    elif resp.status != 200:
                        self.__logger.debug(
                            "traiter_fichiers_reclamation Erreur acces au fichier backup (%d) - on ignore le fichier" % resp.status)
                    else:
                        fichier_backup_disponible = True
                        async for chunk in resp.content.iter_chunked(64 * 1024):
                            output_file.write(chunk)

            # Renommer le fichier .work si present
            path_backup.unlink(missing_ok=True)
            if fichier_backup_disponible:
                path_backup_work.rename(path_backup)
            else:
                # Retirer le fichier vide
                path_backup_work.unlink()

        self.__logger.debug("traiter_fichiers_reclamation Download termine OK")

    async def merge_fichiers_reclamation(self, connection: SQLiteConnection):
        # Charger reclamations
        path_data = pathlib.Path(self.__etat_instance.configuration.dir_consignation, Constantes.DIR_DATA)
        path_reclamations = pathlib.Path(path_data, Constantes.FICHIER_RECLAMATIONS_PRIMAIRES)
        path_reclamations_intermediaire = pathlib.Path(path_data, Constantes.FICHIER_RECLAMATIONS_INTERMEDIAIRES)

        async with SQLiteDetachedReclamationFichierAppend(connection) as reclamation_dao:
            # L'ouverture execute la creation de la db
            self.__logger.debug(
                "__sequence_sync_secondaire Nouvelle base de donnees de sync cree (%s)" % connection.path_database)

            # Lire le fichier de reclamations
            with gzip.open(str(path_reclamations), 'rt') as fichier:
                while True:
                    row_str = await asyncio.to_thread(fichier.readline, 1024)
                    if not row_str:
                        break
                    if self.__stop_event.is_set():
                        raise Exception('stopped')  # Stopped
                    row = json.loads(row_str)
                    await reclamation_dao.ajouter_reclamation(row['fuuid'], row['bucket'], row['taille'], row['etat_fichier'])

            try:
                with path_reclamations_intermediaire.open('rt') as fichier:
                    while True:
                        row_str = await asyncio.to_thread(fichier.readline, 1024)
                        if not row_str:
                            break
                        if self.__stop_event.is_set():
                            raise Exception('stopped')  # Stopped
                        row = json.loads(row_str)
                        await reclamation_dao.ajouter_reclamation(row['fuuid'], row['bucket'], row['taille'], row['etat_fichier'])
            except OSError as e:
                if e.errno == errno.ENOENT:
                    pass  # OK, fichier absent
                else:
                    raise e

            # Commit derniere batch
            await reclamation_dao.commit_batch()

        # Charger fichier de backup (si present)
        path_backup = pathlib.Path(path_data, Constantes.FICHIER_BACKUP)
        try:
            with gzip.open(str(path_backup), 'rt') as fichier:
                async with SQLiteDetachedBackupAppend(connection) as backup_dao:
                    while True:
                        row_str = await asyncio.to_thread(fichier.readline, 1024)
                        if not row_str:
                            break
                        if self.__stop_event.is_set():
                            raise Exception('stopped')  # Stopped
                        row = json.loads(row_str)
                        await backup_dao.ajouter_backup_primaire(row['uuid_backup'], row['domaine'], row['nom_fichier'], row['taille'])

                    # Commit derniere batch
                    await backup_dao.commit_batch()
        except OSError as e:
            if e.errno == errno.ENOENT:
                pass  # OK, fichier absent
            else:
                raise e


        # with self.__etat_instance.sqlite_connection() as connection:
        #     async with SQLiteBatchOperations(connection) as dao:
        #         path_data = pathlib.Path(self.__etat_instance.configuration.dir_consignation, Constantes.DIR_DATA)
        #         path_reclamations = pathlib.Path(path_data, Constantes.FICHIER_RECLAMATIONS_PRIMAIRES)
        #         path_reclamations_intermediaire = pathlib.Path(path_data, Constantes.FICHIER_RECLAMATIONS_INTERMEDIAIRES)
        #         path_backup = pathlib.Path(path_data, Constantes.FICHIER_BACKUP)
        #
        #         async with SQLiteWriteOperations(connection) as dao_write:
        #             await asyncio.to_thread(dao_write.truncate_fichiers_primaire)
        #
        #         # Lire le fichier de reclamations et conserver dans table FICHIERS_PRIMAIRE
        #         with gzip.open(str(path_reclamations), 'rt') as fichier:
        #             while True:
        #                 row_str = await asyncio.to_thread(fichier.readline, 1024)
        #                 if not row_str:
        #                     break
        #                 if self.__stop_event.is_set():
        #                     raise Exception('stopped')  # Stopped
        #                 row = json.loads(row_str)
        #                 commit_done = await dao.ajouter_fichier_primaire(row)
        #                 if commit_done:
        #                     # Ajouter throttle pour permettre acces DB
        #                     await asyncio.sleep(0.5)
        #
        #         # Charger fichier intermediaire si present
        #         try:
        #             with path_reclamations_intermediaire.open('rt') as fichier:
        #                 while True:
        #                     row_str = await asyncio.to_thread(fichier.readline, 1024)
        #                     if not row_str:
        #                         break
        #                     if self.__stop_event.is_set():
        #                         raise Exception('stopped')  # Stopped
        #                     row = json.loads(row_str)
        #                     commit_done = await dao.ajouter_fichier_primaire(row)
        #                     if commit_done:
        #                         # Ajouter throttle pour permettre acces DB
        #                         await asyncio.sleep(0.5)
        #
        #         except OSError as e:
        #             if e.errno == errno.ENOENT:
        #                 pass  # OK, fichier absent
        #             else:
        #                 raise e
        #
        #         # Commit derniere batch
        #         await dao.commit_batch()
        #
        #         # Charger fichier backup si present
        #         async with SQLiteWriteOperations(connection) as dao_write:
        #             await asyncio.to_thread(dao_write.truncate_backup_primaire)
        #
        #         try:
        #             with gzip.open(str(path_backup), 'rt') as fichier:
        #                 while True:
        #                     row_str = await asyncio.to_thread(fichier.readline, 1024)
        #                     if not row_str:
        #                         break
        #                     if self.__stop_event.is_set():
        #                         raise Exception('stopped')  # Stopped
        #                     row = json.loads(row_str)
        #                     await dao.ajouter_backup_primaire(row)
        #         except OSError as e:
        #             if e.errno == errno.ENOENT:
        #                 pass  # OK, fichier absent
        #             else:
        #                 raise e
        #
        #         # Commit derniere batch
        #         await dao.commit_batch()

    # async def creer_operations_sur_secondaire(self):
    #     with self.__etat_instance.sqlite_connection() as connection:
    #         async with SQLiteBatchOperations(connection) as dao:
    #             await dao.marquer_secondaires_reclames()
    #             await asyncio.sleep(2)
    #
    #             await dao.generer_uploads()
    #             await asyncio.sleep(2)
    #
    #             await dao.generer_downloads()
    #
    #     # Declencher les threads d'upload et de download (aucun effect si threads deja actives)
    #     self.__upload_event.set()
    #     self.__download_event.set()

    async def run_upload(self):
        path_database_transferts = pathlib.Path(self.__etat_instance.get_path_data(), Constantes.FICHIER_DATABASE_TRANSFERTS)
        self.__samples_upload = list()  # Reset samples download

        with SQLiteConnection(path_database_transferts, check_same_thread=False) as connection_transfert:
            timeout = aiohttp.ClientTimeout(connect=20)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                while True:
                    async with SQLiteTransfertOperations(connection_transfert) as transfert_dao:
                        job_upload = await asyncio.to_thread(transfert_dao.get_next_upload)
                    if job_upload is None:
                        break
                    if self.__stop_event.is_set():
                        self.__logger.warning("run_upload Annuler upload, stop_event est True")
                        return
                    self.__logger.debug("run_upload Uploader fichier %s" % job_upload)
                    try:
                        await self.upload_fichier_primaire(session, connection_transfert, job_upload)
                    except Exception:
                        self.__logger.exception("run_upload Erreur upload fichier du primaire : %s" % job_upload['fuuid'])

        await self.emettre_etat_upload_termine()

        self.__samples_upload = list()  # Reset samples download
        self.__logger.debug("run_upload Aucunes jobs d'upload restantes - uploads courants termines")

    async def run_download(self):
        path_database = pathlib.Path(self.__etat_instance.get_path_data(), Constantes.FICHIER_DATABASE_TRANSFERTS)
        with SQLiteConnection(path_database, check_same_thread=False) as connection_transfert:
            self.__samples_download = list()  # Reset samples download

            timeout = aiohttp.ClientTimeout(connect=20)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                while True:
                    async with SQLiteTransfertOperations(connection_transfert) as transfert_dao:
                        job_download = await asyncio.to_thread(transfert_dao.get_next_download)
                    if job_download is None:
                        break
                    if self.__stop_event.is_set():
                        self.__logger.warning("run_download Annuler download, stop_event est True")
                        return
                    self.__logger.debug("run_download Downloader fichier %s" % job_download)
                    try:
                        await self.download_fichier_primaire(session, connection_transfert, job_download)
                    except Exception:
                        self.__logger.exception("Erreur download fichier du primaire : %s" % job_download['fuuid'])

        await self.emettre_etat_download_termine()

        self.__samples_download = list()  # Reset samples download
        self.__logger.debug("run_download Aucunes jobs de download restant - downloads courants termines")

    async def download_fichier_primaire(self, session: aiohttp.ClientSession, connection_transfert: SQLiteConnection, fichier: dict):
        self.__download_en_cours = fichier
        self.__download_en_cours['position'] = 0
        fuuid = fichier['fuuid']

        producer = self.__etat_instance.producer
        await asyncio.wait_for(producer.producer_pret().wait(), timeout=1)

        # S'assurer que le fichier n'existe pas deja
        try:
            info_fichier = await self.__consignation.get_info_fichier(fuuid)
            # Le fichier existe deja (aucune exception). Verifier qu'il est manquant.
            if info_fichier['etat_fichier'] != 'manquant':
                # Le fichier n'est pas manquant - annuler le download.
                self.__download_en_cours = None
                async with SQLiteTransfertOperations(connection_transfert) as transfert_dao:
                    await asyncio.to_thread(transfert_dao.supprimer_job_download, fuuid)
                return
        except TypeError:
            pass  # OK, le fichier n'existe pas

        url_primaire = parse_url(self.__etat_instance.url_consignation_primaire)
        url_primaire_reclamations = parse_url(
            '%s/%s/%s' % (url_primaire.url, 'fichiers_transfert', fuuid))

        path_download = pathlib.Path(self.__etat_instance.configuration.dir_consignation, Constantes.DIR_SYNC_DOWNLOAD)
        path_download.mkdir(parents=True, exist_ok=True)
        path_fichier_work = pathlib.Path(path_download, '%s.work' % fuuid)

        date_download_maj = datetime.datetime.utcnow()
        intervalle_download_maj = datetime.timedelta(seconds=5)

        with path_fichier_work.open('wb') as output_file:
            async with session.get(url_primaire_reclamations.url, ssl=self.__etat_instance.ssl_context) as resp:
                if resp.status != 200:
                    self.__logger.warning("Erreur download fichier %s (status %d)" % (fuuid, resp.status))
                    async with SQLiteTransfertOperations(connection_transfert) as transfert_dao:
                        if resp.status == 404:
                            self.__logger.warning(
                                "Erreur download fichier %s - supprimer le download" % fuuid)
                            await asyncio.to_thread(transfert_dao.supprimer_job_download, fuuid)
                        await asyncio.to_thread(transfert_dao.touch_download, fuuid, resp.status)
                    path_fichier_work.unlink()
                    return

                async with SQLiteTransfertOperations(connection_transfert) as transfert_dao:
                    await self.emettre_etat_download(fuuid, transfert_dao, producer)
                date_download_maj = datetime.datetime.utcnow()

                debut_chunk = datetime.datetime.utcnow()
                async for chunk in resp.content.iter_chunked(64 * 1024):
                    if self.__stop_event.is_set():
                        self.__logger.warning("download_fichier_primaire Annuler download, stop_event est True")
                        return

                    output_file.write(chunk)
                    self.__download_en_cours['position'] += len(chunk)

                    # Calculer vitesse transfert
                    now = datetime.datetime.utcnow()
                    duree_transfert = now - debut_chunk
                    self.__samples_download.append({'duree': duree_transfert, 'taille': len(chunk)})
                    while len(self.__samples_download) > CONST_LIMITE_SAMPLES_DOWNLOAD:
                        self.__samples_download.pop(0)  # Detruire vieux samples

                    if now - intervalle_download_maj > date_download_maj:
                        date_download_maj = now
                        async with SQLiteTransfertOperations(connection_transfert) as transfert_dao:
                            await self.emettre_etat_download(fuuid, transfert_dao, producer)

                    # Debut compter pour prochain chunk
                    debut_chunk = now

        async with SQLiteTransfertOperations(connection_transfert) as transfert_dao:
            await self.emettre_etat_download(fuuid, transfert_dao, producer)

        # Consigner le fichier recu
        await self.__consignation.consigner(path_fichier_work, fuuid)

        async with SQLiteTransfertOperations(connection_transfert) as transfert_dao:
            await asyncio.to_thread(transfert_dao.supprimer_job_download, fuuid)

        self.__download_en_cours = None

    async def emettre_etat_download(self, fuuid, transfert_dao: SQLiteTransfertOperations, producer):
        samples = self.__samples_download.copy()
        # Calculer vitesse de transfert
        duree = datetime.timedelta(seconds=0)
        taille = 0
        for s in samples:
            duree += s['duree']
            taille += s['taille']

        secondes = duree / datetime.timedelta(seconds=1)
        if secondes > 0.0:
            taux = round(taille / secondes)  # B/s
        else:
            taux = None

        await asyncio.to_thread(transfert_dao.touch_download, fuuid, None)
        try:
            etat = await asyncio.to_thread(transfert_dao.get_etat_downloads)
            nombre = etat['nombre']
            taille = etat['taille']
        except TypeError:
            # Aucuns downloads
            nombre = None
            taille = None

        try:
            position_en_cours = self.__download_en_cours['position']
            taille_en_cours = self.__download_en_cours['taille']
        except (TypeError, KeyError):
            position_en_cours = None
            taille_en_cours = None

        self.__logger.debug("emettre_etat_download %s fichiers, %s bytes transfere a %s KB/sec (courant: %s/%s)" % (nombre, taille, taux, position_en_cours, taille_en_cours))

        evenement = {
            'termine': False,
            'taille': taille,
            'nombre': nombre,
            'taille_en_cours': taille_en_cours,
            'position_en_cours': position_en_cours,
            'taux': taux,
        }

        await producer.emettre_evenement(
            evenement,
            domaine=Constantes.DOMAINE_FICHIERS, action=Constantes.EVENEMENT_SYNC_DOWNLOAD,
            exchanges=ConstantesMillegrilles.SECURITE_PRIVE
        )

    async def emettre_etat_download_termine(self):
        producer = self.__etat_instance.producer
        await asyncio.wait_for(producer.producer_pret().wait(), timeout=1)
        await producer.emettre_evenement(
            {'termine': True},
            domaine=Constantes.DOMAINE_FICHIERS, action=Constantes.EVENEMENT_SYNC_DOWNLOAD,
            exchanges=ConstantesMillegrilles.SECURITE_PRIVE
        )

    async def run_entretien_transferts(self):
        path_database = pathlib.Path(self.__etat_instance.get_path_data(), Constantes.FICHIER_DATABASE_TRANSFERTS)
        with SQLiteConnection(path_database, check_same_thread=False) as connection:
            async with SQLiteTransfertOperations(connection) as dao:
                await dao.init_database()  # Aucun effet si existe deja
                await dao.entretien_transferts()

        # Entretien repertoire staging/sync/download - supprimer fichiers inactifs

        path_download = pathlib.Path(self.__etat_instance.configuration.dir_consignation, Constantes.DIR_SYNC_DOWNLOAD)
        date_expiration = (datetime.datetime.now() - datetime.timedelta(hours=2)).timestamp()

        try:
            for file in path_download.iterdir():
                stat_file = file.stat()
                if stat_file.st_mtime < date_expiration:
                    self.__logger.info("Supprimer fichier sync download expire %s" % file)
                    file.unlink()
        except OSError as e:
            if e.errno == errno.ENOENT:
                pass  # OK
            else:
                raise e

        if self.__etat_instance.est_primaire is False:
            # Redeclencher transferts sur secondaire
            self.__upload_event.set()
            self.__download_event.set()

    async def upload_fichier_primaire(self, session: aiohttp.ClientSession, connection_transfert: SQLiteConnection, fichier: dict):
        fuuid = fichier['fuuid']

        producer = self.__etat_instance.producer
        await asyncio.wait_for(producer.producer_pret().wait(), timeout=1)

        await self.__consignation.store_pret_wait()

        try:
            event_done = asyncio.Event()
            info_fichier = await self.__consignation.get_info_fichier(fuuid)
            taille_fichier = info_fichier['taille']
            async with self.__consignation.get_fp_fuuid(fuuid) as fichier:
                etat_upload = EtatUpload(fuuid, fichier, self.__stop_event, taille_fichier)

                # Creer un callback pour l'activite d'upload
                dernier_update = datetime.datetime.utcnow()
                intervalle_update = datetime.timedelta(seconds=5)

                async def cb_upload():
                    nonlocal dernier_update
                    now = datetime.datetime.utcnow()
                    if now - intervalle_update > dernier_update:
                        dernier_update = now
                        async with SQLiteTransfertOperations(connection_transfert) as dao_write:
                            await asyncio.to_thread(dao_write.touch_upload, fuuid, None)

                etat_upload.cb_activite = cb_upload

                # Conserver liste precedent de samples pour calculer la vitesse
                etat_upload.samples = self.__samples_upload or list()

                self.__upload_en_cours = etat_upload
                tasks = [
                    uploader_fichier(session, self.__etat_instance, event_done, etat_upload),
                    self.__run_emettre_etat_upload(fuuid, producer, event_done),
                ]
                await asyncio.gather(*tasks)

                # Conserver liste samples pour calculer la vitesse
                self.__samples_upload = etat_upload.samples

            # Transfert termine. Supprimer job d'upload
            async with SQLiteTransfertOperations(connection_transfert) as dao_write:
                await asyncio.to_thread(dao_write.supprimer_job_upload, fuuid)
        except ClientResponseError as e:
            if e.status == 409:
                self.__logger.info("upload_fichier_primaire Le fichier %s existe deja sur le serveur - OK, terminer job immediatement" % fuuid)
                async with SQLiteTransfertOperations(connection_transfert) as dao_write:
                    await asyncio.to_thread(dao_write.supprimer_job_upload, fuuid)
        except FileNotFoundError as e:
            self.__logger.info(
                "upload_fichier_primaire Le fichier %s n'existe pas localement : %s" % (fuuid, e))
            async with SQLiteTransfertOperations(connection_transfert) as dao_write:
                await asyncio.to_thread(dao_write.supprimer_job_upload, fuuid)
        except Exception:
            self.__logger.exception('upload_fichier_primaire Erreur upload fichier vers primaire')
            async with SQLiteTransfertOperations(connection_transfert) as dao_write:
                await asyncio.to_thread(dao_write.touch_upload, fuuid, -1)

        self.__upload_en_cours = None

    async def __run_emettre_etat_upload(self, fuuid: str, producer, event_stop: asyncio.Event):
        with self.__etat_instance.sqlite_connection() as connection:
            while event_stop.is_set() is False:
                try:
                    await self.emettre_etat_upload(fuuid, connection, producer)
                    await asyncio.wait_for(event_stop.wait(), timeout=5)
                except asyncio.TimeoutError:
                    pass  # OK
                except asyncio.CancelledError:
                    return  # Stopped

    async def emettre_etat_upload(self, fuuid, connection: SQLiteConnection, producer):

        path_database_transferts = pathlib.Path(self.__etat_instance.get_path_data(),
                                                Constantes.FICHIER_DATABASE_TRANSFERTS)

        upload_en_cours = self.__upload_en_cours
        if upload_en_cours is not None:
            try:
                position_en_cours = upload_en_cours.position
                taille_en_cours = upload_en_cours.taille
            except (TypeError, KeyError):
                # pct = None
                position_en_cours = None
                taille_en_cours = None

            samples = upload_en_cours.samples.copy()
            # Calculer vitesse de transfert
            duree = datetime.timedelta(seconds=0)
            taille = 0
            for s in samples:
                duree += s['duree']
                taille += s['taille']

            secondes = duree / datetime.timedelta(seconds=1)
            if secondes > 0.0:
                taux = round(taille / secondes)  # B/s
            else:
                taux = None

        with SQLiteConnection(path_database_transferts, check_same_thread=False) as connection_transferts:
            async with SQLiteTransfertOperations(connection_transferts) as transferts_dao:
                await asyncio.to_thread(transferts_dao.touch_upload, fuuid, None)

            async with SQLiteTransfertOperations(connection_transferts) as transferts_dao:
                try:
                    etat = await asyncio.to_thread(transferts_dao.get_etat_uploads)
                    nombre = etat['nombre']
                    taille = etat['taille']
                except TypeError:
                    # Aucuns downloads
                    nombre = None
                    taille = None

        self.__logger.debug("emettre_etat_upload %s fichiers, %s bytes transfere a %s KB/sec (courant: %s/%s)" %
                            (nombre, taille, taux, position_en_cours, taille_en_cours))

        evenement = {
            'termine': False,
            'taille': taille,
            'nombre': nombre,
            'taille_en_cours': taille_en_cours,
            'position_en_cours': position_en_cours,
            'taux': taux,
        }

        await producer.emettre_evenement(
            evenement,
            domaine=Constantes.DOMAINE_FICHIERS, action=Constantes.EVENEMENT_SYNC_UPLOAD,
            exchanges=ConstantesMillegrilles.SECURITE_PRIVE
        )

    async def emettre_etat_upload_termine(self):
        producer = self.__etat_instance.producer
        await asyncio.wait_for(producer.producer_pret().wait(), timeout=1)
        await producer.emettre_evenement(
            {'termine': True},
            domaine=Constantes.DOMAINE_FICHIERS, action=Constantes.EVENEMENT_SYNC_UPLOAD,
            exchanges=ConstantesMillegrilles.SECURITE_PRIVE
        )

    async def ajouter_fichier_primaire(self, commande: dict):
        fuuid = commande['fuuid']
        self.__logger.debug("ajouter_fichier_primaire Conserver nouveau fichier consigne su primaire %s" % fuuid)

        path_database_transferts = pathlib.Path(self.__etat_instance.get_path_data(),
                                                Constantes.FICHIER_DATABASE_TRANSFERTS)

        with self.__etat_instance.sqlite_connection() as connection:

            async with SQLiteWriteOperations(connection) as dao:
                # Ajouter le fuuid a la liste de fichiers manquants
                ajoute = await asyncio.to_thread(dao.ajouter_fichier_manquant, fuuid)

            if ajoute:
                # Tenter d'obtenir la taille du fichier pour ajouter a la liste FICHIERS_PRIMAIRE et DOWNLOADS
                url_primaire = parse_url(self.__etat_instance.url_consignation_primaire)
                url_primaire_reclamations = parse_url(
                    '%s/%s/%s' % (url_primaire.url, 'fichiers_transfert', fuuid))

                taille_str = None
                timeout = aiohttp.ClientTimeout(connect=5)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.head(url_primaire_reclamations.url, ssl=self.__etat_instance.ssl_context) as resp:
                        if resp.status == 200:
                            taille_str = resp.headers.get('Content-Length')
                        else:
                            self.__logger.info("ajouter_fichier_primaire Nouveau fichier %s non accessible sur primaire (status:%d)" % (fuuid, resp.status))

                if taille_str is not None:
                    self.__logger.debug("ajouter_fichier_primaire Fichier %s accessible pour download, taille %s" % (fuuid, taille_str))

                with SQLiteConnection(path_database_transferts, check_same_thread=False) as connection_transferts:
                    taille_int = int(taille_str)
                    async with SQLiteTransfertOperations(connection_transferts) as transferts_dao:
                        await asyncio.to_thread(transferts_dao.ajouter_download_primaire, fuuid, taille_int)

                # Declencher thread download au besoin
                self.__download_event.set()

        pass

    async def ajouter_upload_secondaire(self, fuuid: str):
        """ Ajouter conditionnellement un upload vers le primaire """

        path_database_transferts = pathlib.Path(self.__etat_instance.get_path_data(),
                                                Constantes.FICHIER_DATABASE_TRANSFERTS)

        with self.__etat_instance.sqlite_connection() as connection_fichiers:
            with SQLiteConnection(path_database_transferts, check_same_thread=False) as connection_transferts:
                # Verifier si le fichier est present dans FICHIERS_PRIMAIRE
                async with SQLiteTransfertOperations(connection_transferts) as dao_write:
                    ajoute = await dao_write.ajouter_upload_secondaire_conditionnel(fuuid, connection_fichiers)
        if ajoute:
            self.__logger.debug("ajouter_upload_secondaire Declencher upload pour fuuid %s" % fuuid)
            self.__upload_event.set()

    async def run_sync_backup(self):
        if self.__etat_instance.est_primaire is False:
            path_database_transferts = pathlib.Path(self.__etat_instance.get_path_data(),
                                                    Constantes.FICHIER_DATABASE_TRANSFERTS)

            with SQLiteConnection(path_database_transferts, check_same_thread=False) as connection_transfert:
                timeout = aiohttp.ClientTimeout(connect=20, total=900)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    await self.__consignation.upload_backups_primaire(connection_transfert, session)
                    await self.download_backups_primaire(connection_transfert, session)

    async def download_backups_primaire(self, connection_transfert: SQLiteConnection, session: aiohttp.ClientSession):
        """ Downloader les fichiers de backup qui sont manquants localement """

        while True:
            async with SQLiteTransfertOperations(connection_transfert) as dao:
                backups = await asyncio.to_thread(dao.get_batch_backups_primaire)
            if len(backups) == 0:
                break  # Done

            # Verifier si le backup existe localement
            for backup in backups:
                try:
                    info = await self.__consignation.get_info_fichier_backup(
                        backup['uuid_backup'], backup['domaine'], backup['nom_fichier'])
                except FileNotFoundError:
                    # Downloader le backup
                    try:
                        await self.download_backup(session, backup)
                    except ClientResponseError as e:
                        if 500 <= e.status < 600:
                            self.__logger.warning("download_backups_primaire Erreur serveur - on arrete le transfert")
                            raise e
                        self.__logger.info("download_backups_primaire Backup a downloader %s %s n'est pas disponible (%d)" % (backup['uuid_backup'], backup['nom_fichier'], e.status))
                    except (InvalidSignature, PathValidationError):
                        self.__logger.exception("download_backups_primaire Erreur validation backup %s - SKIP", backup['nom_fichier'])

    async def download_backup(self, session: aiohttp.ClientSession, backup: dict):
        uuid_backup = backup['uuid_backup']
        domaine = backup['domaine']
        nom_fichier = backup['nom_fichier']

        url_consignation_primaire = self.__etat_instance.url_consignation_primaire
        url_backup = '%s/fichiers_transfert/backup' % url_consignation_primaire
        url_fichier = f"{url_backup}/{uuid_backup}/{domaine}/{nom_fichier}"

        with tempfile.TemporaryFile() as output:
            resp = await session.get(url_fichier, ssl=self.__etat_instance.ssl_context)
            resp.raise_for_status()

            # Conserver le contenu
            async for chunk in resp.content.iter_chunked(64 * 1024):
                output.write(chunk)

            # Verifier le fichier (signature)
            output.seek(0)
            with gzip.open(output, 'rt') as fichier:
                contenu_backup = json.load(fichier)

            await self.__etat_instance.validateur_message.verifier(contenu_backup)

            output.seek(0)
            await self.__consignation.conserver_backup(output, uuid_backup, domaine, nom_fichier)
