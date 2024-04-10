import shutil

import aiohttp
import asyncio
import logging
import json
import os

from operator import attrgetter
from typing import Optional

from millegrilles_messages.messages import Constantes as ConstantesMillegrilles
from millegrilles_backup import Constantes
from millegrilles_backup.EtatBackup import EtatBackup


class ConsignationHandler:
    """
    Upload les fichiers de backup locaux vers un serveur de consignation.
    """

    def __init__(self, stop_event: asyncio.Event, etat_instance: EtatBackup):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__stop_event = stop_event
        self.__etat_instance = etat_instance

        self.__trigger_consignation: Optional[asyncio.Event] = None
        self.__url_consignation: Optional[str] = None
        self.__queue_upload: Optional[asyncio.Queue] = None

        self.__session_http: aiohttp.ClientSession = None

    async def configurer(self):
        self.__trigger_consignation = asyncio.Event()
        self.__queue_upload = asyncio.Queue(maxsize=1000)

    async def trigger(self):
        self.__trigger_consignation.set()

    async def run(self):
        await asyncio.gather(
            self.thread_upload(),
            self.thread_preparer(),
            self.thread_entretien()
        )

    async def thread_upload(self):
        timeout = aiohttp.ClientTimeout(connect=5, total=120)
        stop_wait_task = asyncio.create_task(self.__stop_event.wait())
        while self.__stop_event.is_set() is False:
            done, pending = await asyncio.wait([stop_wait_task, self.__queue_upload.get()], return_when=asyncio.FIRST_COMPLETED)
            if self.__stop_event.is_set() is True:
                break  # Fermeture de l'application

            # On a recu un fichier a uploader
            task_item_upload = done.pop()
            item_upload: dict = task_item_upload.result()
            self.__logger.debug("Uploader fichier %s" % item_upload)

            if self.__session_http is None or self.__session_http.closed:
                self.__session_http = aiohttp.ClientSession(timeout=timeout)

            try:
                await self.upload_fichier(item_upload)
            except:
                self.__logger.exception("Erreur upload fichier %s" % item_upload)

    async def thread_preparer(self):
        self.__logger.info("run Demarrer ConsignationHandler")

        timeout = 15  # Timeout initial

        stop_wait_task = asyncio.create_task(self.__stop_event.wait())
        while self.__stop_event.is_set() is False:
            self.__logger.debug("run Cycle wait")
            consignation_wait_task = asyncio.create_task(self.__trigger_consignation.wait())
            await asyncio.wait(
                [stop_wait_task, consignation_wait_task],
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED
            )

            timeout = 3600  # 1 heure

            if self.__stop_event.is_set() is True:
                break

            consignation_wait_task.cancel()
            self.__trigger_consignation.clear()

            try:
                await self.run_consignation()
            except:
                self.__logger.exception("Erreur execution upload backup vers consignation")
                try:
                    # Pause pour 30 secondes
                    await asyncio.wait_for(self.__stop_event.wait(), 30)
                except asyncio.TimeoutError:
                    pass

        self.__logger.info("run Arreter ConsignationHandler")

    async def thread_entretien(self):
        timeout = 10
        while self.__stop_event.is_set() is False:
            try:
                await asyncio.wait_for(self.__stop_event.wait(), timeout=timeout)
                break  # Fermer application
            except asyncio.TimeoutError:
                pass

            timeout = 3600

            try:
                await self.entretien_consignation()
            except:
                self.__logger.exception("Erreur emission message entretien consignation")
                await asyncio.sleep(15)

    async def run_consignation(self):
        self.__logger.debug("run_consignation Debut")
        # Charger information de backup courant
        uuid_backup = await self.charger_info_backup_courant()
        self.__logger.debug("uuid_backup courant : %s" % uuid_backup)

        # Charger le URL de consignation courant
        self.__url_consignation = await self.charger_consignation_url()
        self.__logger.debug("Consignation : utiliser url %s" % self.__url_consignation)

        configuration = self.__etat_instance.configuration
        dir_backup = os.path.join(configuration.dir_backup, 'transactions')

        # Parcourir liste de domaines a synchroniser
        try:
            for path_item in os.listdir(dir_backup):
                path_complet = os.path.join(dir_backup, path_item)
                if os.path.isdir(path_complet) is False:
                    continue  # On veut juste les domaines

                # Comparer liste locale avec serveur
                await self.queue_fichiers_a_uploader(uuid_backup, dir_backup, path_item)

                # Uploader fichiers manquants
        except FileNotFoundError:
            self.__logger.info("run_consignation backup %s absent, skip entretien" % dir_backup)
            return

        self.__logger.debug("run_consignation Fin")

    async def charger_info_backup_courant(self):
        configuration = self.__etat_instance.configuration

        dir_backup = configuration.dir_backup
        try:
            with open(os.path.join(dir_backup, 'transactions', Constantes.FICHIER_BACKUP_COURANT), 'r') as fichier:
                config = json.load(fichier)
        except FileNotFoundError:
            self.__logger.info("Repertoire de backup %s absent ou vide (aucuns backup disponibles)", dir_backup)
            return

        if config['en_cours'] is True:
            raise Exception('Backup en cours - ABORT')

        return config['uuid_backup']

    async def charger_consignation_url(self):
        producer = self.__etat_instance.producer
        await asyncio.wait_for(producer.producer_pret().wait(), 30)

        reponse = await producer.executer_requete(
            dict(), 'CoreTopologie', 'getConsignationFichiers', exchange="2.prive")

        try:
            return reponse.parsed['consignation_url']
        except Exception as e:
            self.__logger.exception("Erreur chargement URL consignation")

    async def queue_fichiers_a_uploader(self, uuid_backup: str, path_backup: str, domaine: str):
        self.__logger.debug("queue_fichiers_a_uploader fichiers domaine %s (backup: %s)" % (domaine, uuid_backup))
        path_domaine = os.path.join(path_backup, domaine)

        liste_fichiers = list()

        for fichier in os.listdir(path_domaine):
            path_fichier = os.path.join(path_domaine, fichier)
            if os.path.isfile(path_fichier):
                info_fichier = {
                    'fullpath': path_fichier,
                    'nom_fichier': fichier,
                    'domaine': domaine,
                    'uuid_backup': uuid_backup,
                }
                liste_fichiers.append(info_fichier)

        url_verification = '%s/%s' % (self.__url_consignation, 'fichiers_transfert/backup/verifierFichiers')
        ssl_context = self.__etat_instance.ssl_context

        timeout = aiohttp.ClientTimeout(connect=5, total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Soumettre les fichiers au serveur pour recuperer la liste de ceux qui manquent
            while len(liste_fichiers) > 0:
                batch_fichiers = liste_fichiers[:100]
                liste_fichiers = liste_fichiers[100:]

                requete = {
                    'uuid_backup': uuid_backup,
                    'domaine': domaine,
                    'fichiers': [f['nom_fichier'] for f in batch_fichiers]
                }

                reponse = await session.post(url_verification, json=requete, ssl=ssl_context)
                self.__logger.debug("reponse verification fichiers status : %s", reponse.status)

                if reponse.status != 200:
                    raise Exception('Erreur verification presence fichiers (status : %d), ABORT' % reponse.status)

                # Comparer la liste recue et la batch emise - ajouter transfert pour fichiers qui ne sont pas presents
                reponse_fichiers = await reponse.json()
                for fichier_info in batch_fichiers:
                    nom_fichier = fichier_info['nom_fichier']
                    present = reponse_fichiers.get(nom_fichier)
                    if present is not True:
                        # Ajouter liste des fichiers manquants a la Q de transfert
                        await self.__queue_upload.put(fichier_info)

    async def upload_fichier(self, info_fichier: dict):
        uuid_backup = info_fichier['uuid_backup']
        domaine = info_fichier['domaine']
        nom_fichier = info_fichier['nom_fichier']
        path_fichier = info_fichier['fullpath']

        url_upload = f"{self.__url_consignation}/fichiers_transfert/backup/upload/{uuid_backup}/{domaine}/{nom_fichier}"
        self.__logger.debug("Uploader fichier backup vers %s" % url_upload)

        with open(path_fichier, 'rb') as fichier:
            reponse = await self.__session_http.put(url_upload, data=fichier, ssl=self.__etat_instance.ssl_context)

        self.__logger.debug("Reponse upload fichier status : %s" % reponse.status)

    async def entretien_consignation(self):
        self.__logger.debug("entretien_consignation Debut")

        if self.__etat_instance.backup_inhibe:
            self.__logger.info("entretien_consignation Annule, backup/restauration en cours")
            return

        # Parcourir tous les backup, charger info
        configuration = self.__etat_instance.configuration
        dir_backups = configuration.dir_backup

        liste_backups = list()

        for backup_set in os.listdir(dir_backups):
            path_dir = os.path.join(dir_backups, backup_set)
            if os.path.isdir(path_dir):
                # Charger info
                path_info = os.path.join(path_dir, Constantes.FICHIER_BACKUP_COURANT)
                try:
                    with open(path_info, 'r') as fichier:
                        info_backup = json.load(fichier)
                        date_backup = info_backup['date']  # S'assurer d'avoir la date dans le backup
                        liste_backups.append(info_backup)
                except (FileNotFoundError, KeyError) as e:
                    # On n'inclue pas ce repertoire, il va etre supprime
                    self.__logger.warning("Erreur lecture repertoire backup %s, pas inclus pour conserver : ERR %s" % (path_dir, e))

        # Trier les backups par date
        liste_backups.sort(key=lambda b: b['date'])

        if len(liste_backups) > 5:
            # Conserver les 3 plus recents
            backups_recents = liste_backups[-5:]
        else:
            backups_recents = liste_backups
        uuid_backups_recents = [b['uuid_backup'] for b in backups_recents]

        for backup_set in os.listdir(dir_backups):
            if backup_set == 'transactions':
                continue  # Skip, c'est le repertoire backup courant

            path_dir = os.path.join(dir_backups, backup_set)
            if os.path.isdir(path_dir):
                if backup_set not in uuid_backups_recents:
                    self.__logger.info("Supprimer vieux backup %s" % path_dir)
                    shutil.rmtree(path_dir)

        await self.emettre_message_entretien_consignation(uuid_backups_recents)

    async def emettre_message_entretien_consignation(self, uuid_backups: list):
        self.__logger.debug("emettre_message_entretien_consignation Debut")

        commande = {'uuid_backups': uuid_backups}
        producer = self.__etat_instance.producer
        await asyncio.wait_for(producer.producer_pret().wait(), 5_000)
        await producer.executer_commande(
            commande, ConstantesMillegrilles.DOMAINE_FICHIERS, 'entretienBackup',
            exchange=ConstantesMillegrilles.SECURITE_PRIVE)
