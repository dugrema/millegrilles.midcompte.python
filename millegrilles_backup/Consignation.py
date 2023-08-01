import aiohttp
import asyncio
import logging
import json
import os

from typing import Optional

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
        await asyncio.gather(self.thread_upload(), self.thread_preparer())

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

        timeout = 10  # Timeout initial, 10 secondes

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

        self.__logger.info("run Arreter ConsignationHandler")

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
        for path_item in os.listdir(dir_backup):
            path_complet = os.path.join(dir_backup, path_item)
            if os.path.isdir(path_complet) is False:
                continue  # On veut juste les domaines

            # Comparer liste locale avec serveur
            await self.queue_fichiers_a_uploader(uuid_backup, dir_backup, path_item)

            # Uploader fichiers manquants

        self.__logger.debug("run_consignation Fin")

    async def charger_info_backup_courant(self):
        configuration = self.__etat_instance.configuration

        dir_backup = configuration.dir_backup
        with open(os.path.join(dir_backup, 'transactions', Constantes.FICHIER_BACKUP_COURANT), 'r') as fichier:
            config = json.load(fichier)

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