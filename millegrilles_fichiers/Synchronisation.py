import aiohttp
import asyncio
import datetime
import errno
import gzip
import json
import logging
import pathlib

from typing import Optional
from urllib3.util import parse_url
from aiohttp.client_exceptions import ClientResponseError

from millegrilles_messages.messages import Constantes as ConstantesMillegrilles

from millegrilles_fichiers import Constantes
# from millegrilles_fichiers.Consignation import ConsignationHandler
from millegrilles_fichiers.EtatFichiers import EtatFichiers
from millegrilles_fichiers.ConsignationStore import EntretienDatabase
from millegrilles_fichiers.UploadFichiersPrimaire import uploader_fichier, EtatUpload

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

        self.__download_en_cours: Optional[dict] = None
        self.__samples_download = list()  # Utilise pour calcul de vitesse
        self.__upload_en_cours: Optional[EtatUpload] = None
        self.__samples_upload = list()  # Utilise pour calcul de vitesse

    def demarrer_sync_primaire(self):
        self.__sync_event_primaire.set()

    def demarrer_sync_secondaire(self):
        self.__sync_event_secondaire.set()

    async def run(self):
        self.__sync_event_primaire = asyncio.Event()
        self.__sync_event_secondaire = asyncio.Event()
        self.__reception_fuuids_reclames = asyncio.Queue(maxsize=3)
        self.__upload_event: Optional[asyncio.Event] = asyncio.Event()
        self.__download_event: Optional[asyncio.Event] = asyncio.Event()

        await asyncio.gather(
            self.thread_sync_primaire(),
            self.thread_sync_secondaire(),
            self.thread_traiter_fuuids_reclames(),
            self.thread_upload(),
            self.thread_download(),
            self.thread_entretien_transferts(),
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

    async def thread_upload(self):
        pending = {self.__stop_event.wait()}
        while self.__stop_event.is_set() is False:
            pending.add(self.__upload_event.wait())
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            if self.__stop_event.is_set():
                break  # Done
            try:
                await self.run_upload()
            except Exception:
                self.__logger.exception("thread_upload Erreur synchronisation")

            self.__upload_event.clear()

    async def thread_download(self):
        pending = {self.__stop_event.wait()}
        while self.__stop_event.is_set() is False:
            pending.add(self.__download_event.wait())
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            if self.__stop_event.is_set():
                break  # Done
            try:
                await self.run_download()
            except Exception:
                self.__logger.exception("thread_download Erreur synchronisation")

            self.__download_event.clear()

    async def thread_entretien_transferts(self):
        stop_coro = self.__stop_event.wait()
        while self.__stop_event.is_set() is False:
            try:
                await self.run_entretien_transferts()
            except Exception:
                self.__logger.exception("thread_download Erreur synchronisation")
            await asyncio.wait([stop_coro], timeout=300)

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

        # Generer la liste des fichiers de backup
        await self.__consignation.generer_backup_sync()

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
        try:
            await self.download_fichiers_reclamation()
        except aiohttp.client.ClientResponseError as e:
            if e.status == 404:
                self.__logger.error("__sequence_sync_secondaire Fichier de reclamation primaire n'est pas disponible (404)")
            else:
                self.__logger.error(
                    "__sequence_sync_secondaire Fichier de reclamation primaire non accessible (%d)" % e.status)
            return  # Abandonner la sync

        # Merge information dans database
        await self.merge_fichiers_reclamation()

        # Ajouter manquants, marquer fichiers reclames
        # Marquer orphelins, determiner downloads et upload
        await self.creer_operations_sur_secondaire()

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

    async def merge_fichiers_reclamation(self):
        # Utiliser thread pour traiter les fichiers et database sans bloquer
        await asyncio.to_thread(self.__run_merge_fichiers_reclamation)

    def __run_merge_fichiers_reclamation(self):
        with EntretienDatabase(self.__etat_instance) as entretien_db:
            path_data = pathlib.Path(self.__etat_instance.configuration.dir_consignation, Constantes.DIR_DATA)
            path_reclamations = pathlib.Path(path_data, Constantes.FICHIER_RECLAMATIONS_PRIMAIRES)
            path_reclamations_intermediaire = pathlib.Path(path_data, Constantes.FICHIER_RECLAMATIONS_INTERMEDIAIRES)
            path_backup = pathlib.Path(path_data, Constantes.FICHIER_BACKUP)

            entretien_db.truncate_fichiers_primaire()

            # Lire le fichier de reclamations et conserver dans table FICHIERS_PRIMAIRE
            with gzip.open(str(path_reclamations), 'rt') as fichier:
                while True:
                    row_str = fichier.readline(1024)
                    if not row_str:
                        break
                    row = json.loads(row_str)
                    entretien_db.ajouter_fichier_primaire(row)

            # Charger fichier intermediaire si present
            try:
                with path_reclamations_intermediaire.open('rt') as fichier:
                    while True:
                        row_str = fichier.readline(1024)
                        if not row_str:
                            break
                        row = json.loads(row_str)
                        entretien_db.ajouter_fichier_primaire(row)
            except OSError as e:
                if e.errno == errno.ENOENT:
                    pass  # OK, fichier absent
                else:
                    raise e

            # Commit derniere batch
            entretien_db.commit_fichiers_primaire()

            # Charger fichier backup si present
            entretien_db.truncate_backup_primaire()
            try:
                with gzip.open(str(path_backup), 'rt') as fichier:
                    while True:
                        row_str = fichier.readline(1024)
                        if not row_str:
                            break
                        row = json.loads(row_str)
                        entretien_db.ajouter_backup_primaire(row)
            except OSError as e:
                if e.errno == errno.ENOENT:
                    pass  # OK, fichier absent
                else:
                    raise e

            # Commit derniere batch
            entretien_db.commit_backup_primaire()

    async def creer_operations_sur_secondaire(self):
        await asyncio.to_thread(self.__creer_operations_sur_secondaire)

    def __creer_operations_sur_secondaire(self):
        with EntretienDatabase(self.__etat_instance) as entretien_db:
            entretien_db.marquer_secondaires_reclames()
            entretien_db.generer_uploads()
            entretien_db.generer_downloads()

            # Declencher les threads d'upload et de download (aucun effect si threads deja actives)
            self.__upload_event.set()
            self.__download_event.set()

    async def run_upload(self):
        with EntretienDatabase(self.__etat_instance, check_same_thread=False) as entretien_db:
            self.__samples_upload = list()  # Reset samples download

            timeout = aiohttp.ClientTimeout(connect=20)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                while True:
                    job_upload = await asyncio.to_thread(entretien_db.get_next_upload)
                    if job_upload is None:
                        break
                    if self.__stop_event.is_set():
                        self.__logger.warning("run_upload Annuler upload, stop_event est True")
                        return
                    self.__logger.debug("run_upload Uploader fichier %s" % job_upload)
                    try:
                        await self.upload_fichier_primaire(session, entretien_db, job_upload)
                    except Exception:
                        self.__logger.exception("run_upload Erreur upload fichier du primaire : %s" % job_upload['fuuid'])

        await self.emettre_etat_upload_termine()

        self.__samples_upload = list()  # Reset samples download
        self.__logger.debug("run_upload Aucunes jobs d'upload restantes - uploads courants termines")

    async def run_download(self):
        with EntretienDatabase(self.__etat_instance, check_same_thread=False) as entretien_db:
            self.__samples_download = list()  # Reset samples download

            timeout = aiohttp.ClientTimeout(connect=20)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                while True:
                    job_download = await asyncio.to_thread(entretien_db.get_next_download)
                    if job_download is None:
                        break
                    if self.__stop_event.is_set():
                        self.__logger.warning("run_download Annuler download, stop_event est True")
                        return
                    self.__logger.debug("run_download Downloader fichier %s" % job_download)
                    try:
                        await self.download_fichier_primaire(session, entretien_db, job_download)
                    except Exception:
                        self.__logger.exception("Erreur download fichier du primaire : %s" % job_download['fuuid'])

        await self.emettre_etat_download_termine()

        self.__samples_download = list()  # Reset samples download
        self.__logger.debug("run_download Aucunes jobs de download restant - downloads courants termines")

    async def download_fichier_primaire(self, session: aiohttp.ClientSession, entretien_db: EntretienDatabase, fichier: dict):
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
                await asyncio.to_thread(entretien_db.supprimer_job_download, fuuid)
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
                    await asyncio.to_thread(entretien_db.touch_download, fuuid, resp.status)
                    path_fichier_work.unlink()
                    return

                await self.emettre_etat_download(fuuid, entretien_db, producer)
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
                        await self.emettre_etat_download(fuuid, entretien_db, producer)

                    # Debut compter pour prochain chunk
                    debut_chunk = now

        # Consigner le fichier recu
        await self.__consignation.consigner(path_fichier_work, fuuid)
        await asyncio.to_thread(entretien_db.supprimer_job_download, fuuid)
        self.__download_en_cours = None

    async def emettre_etat_download(self, fuuid, entretien_db: EntretienDatabase, producer):
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

        await asyncio.to_thread(entretien_db.touch_download, fuuid, None)
        try:
            etat = await asyncio.to_thread(entretien_db.get_etat_downloads)
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
        with EntretienDatabase(self.__etat_instance, check_same_thread=False) as entretien_db:
            await asyncio.to_thread(entretien_db.entretien_transferts)

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

    async def upload_fichier_primaire(self, session: aiohttp.ClientSession, entretien_db: EntretienDatabase, fichier: dict):
        fuuid = fichier['fuuid']

        producer = self.__etat_instance.producer
        await asyncio.wait_for(producer.producer_pret().wait(), timeout=1)

        try:
            event_done = asyncio.Event()
            info_fichier = await self.__consignation.get_info_fichier(fuuid)
            taille_fichier = info_fichier['taille']
            async with self.__consignation.get_fp_fuuid(fuuid) as fichier:
                etat_upload = EtatUpload(fuuid, fichier, self.__stop_event, taille_fichier)

                # Conserver liste precedent de samples pour calculer la vitesse
                etat_upload.samples = self.__samples_upload or list()

                self.__upload_en_cours = etat_upload
                pending = {
                    uploader_fichier(session, self.__etat_instance, etat_upload),
                    self.__run_emettre_etat_upload(fuuid, entretien_db, producer, event_done),
                }
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

                # Conserver liste samples pour calculer la vitesse
                self.__samples_upload = etat_upload.samples

                for p in pending:
                    p.cancel()
                for d in done:
                    e = d.exception()
                    if e:
                        raise e
                while len(pending) > 0:
                    done, pending = await asyncio.wait(pending, timeout=5)

            # Transfert termine. Supprimer job d'upload
            await asyncio.to_thread(entretien_db.supprimer_job_upload, fuuid)
        except ClientResponseError as e:
            if e.status == 409:
                self.__logger.info("upload_fichier_primaire Le fichier %s existe deja sur le serveur - OK, terminer job immediatement" % fuuid)
                await asyncio.to_thread(entretien_db.supprimer_job_upload, fuuid)
        except Exception:
            self.__logger.exception('upload_fichier_primaire Erreur upload fichier vers primaire')
            await asyncio.to_thread(entretien_db.touch_upload, fuuid, -1)

        self.__upload_en_cours = None

    async def __run_emettre_etat_upload(self, fuuid: str, entretien_db, producer, event_stop: asyncio.Event):
        while event_stop.is_set() is False:
            try:
                await self.emettre_etat_upload(fuuid, entretien_db, producer)
                await asyncio.wait_for(event_stop.wait(), timeout=5)
            except asyncio.TimeoutError:
                pass  # OK

    async def emettre_etat_upload(self, fuuid, entretien_db: EntretienDatabase, producer):

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

        await asyncio.to_thread(entretien_db.touch_upload, fuuid, None)
        try:
            etat = await asyncio.to_thread(entretien_db.get_etat_uploads)
            nombre = etat['nombre']
            taille = etat['taille']
        except TypeError:
            # Aucuns downloads
            nombre = None
            taille = None

        self.__logger.debug("emettre_etat_upload %s fichiers, %s bytes transfere a %s KB/sec (courant: %s/%s)" % (nombre, taille, taux, position_en_cours, taille_en_cours))

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

        with EntretienDatabase(self.__etat_instance, check_same_thread=False) as entretien_db:

            # Ajouter le fuuid a la liste de fichiers manquants
            ajoute = await asyncio.to_thread(entretien_db.ajouter_fichier_manquant, fuuid)

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

                taille_int = int(taille_str)
                await asyncio.to_thread(entretien_db.ajouter_download_primaire, fuuid, taille_int)

                # Declencher thread download au besoin
                self.__download_event.set()

        pass

    async def ajouter_upload_secondaire(self, fuuid: str):
        """ Ajouter conditionnellement un upload vers le primaire """
        with EntretienDatabase(self.__etat_instance, check_same_thread=False) as entretien_db:
            # Verifier si le fichier est present dans FICHIERS_PRIMAIRE
            ajoute = await asyncio.to_thread(entretien_db.ajouter_upload_secondaire_conditionnel, fuuid)
            if ajoute:
                self.__logger.debug("ajouter_upload_secondaire Declencher upload pour fuuid %s" % fuuid)
                self.__upload_event.set()
