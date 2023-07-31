import asyncio
import logging
import json
import os

from typing import Optional

from millegrilles_messages.MilleGrillesConnecteur import EtatInstance
from millegrilles_backup import Constantes


class ConsignationHandler:
    """
    Upload les fichiers de backup locaux vers un serveur de consignation.
    """

    def __init__(self, stop_event: asyncio.Event, etat_instance: EtatInstance):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__stop_event = stop_event
        self.__etat_instance = etat_instance

        self.__trigger_consignation: Optional[asyncio.Event] = None
        self.__url_consignation: Optional[str] = None

        self.__etat_instance.ajouter_listener_backup_complete(self.trigger)

    async def configurer(self):
        self.__trigger_consignation = asyncio.Event()

    async def trigger(self):
        self.__trigger_consignation.set()

    async def run(self):
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
