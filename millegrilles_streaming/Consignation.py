import shutil

import aiohttp
import asyncio
import logging
import json
import os

from typing import Optional

from millegrilles_messages.messages import Constantes as ConstantesMillegrilles
from millegrilles_streaming import Constantes
from millegrilles_streaming.EtatStreaming import EtatStreaming


class ConsignationHandler:
    """
    Download et dechiffre les fichiers de media a partir d'un serveur de consignation
    """

    def __init__(self, stop_event: asyncio.Event, etat_instance: EtatStreaming):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__stop_event = stop_event
        self.__etat_instance = etat_instance

        self.__url_consignation: Optional[str] = None

        self.__session_http_download: Optional[aiohttp.ClientSession] = None
        self.__session_http_requests: Optional[aiohttp.ClientSession] = None

    async def ouvrir_sessions(self):
        if self.__session_http_download is None or self.__session_http_download.closed:
            timeout = aiohttp.ClientTimeout(connect=5, total=300)
            self.__session_http_download = aiohttp.ClientSession(timeout=timeout)

        if self.__session_http_requests is None or self.__session_http_requests.closed:
            timeout_requests = aiohttp.ClientTimeout(connect=5, total=15)
            self.__session_http_requests = aiohttp.ClientSession(timeout=timeout_requests)

    async def charger_consignation_url(self):
        producer = self.__etat_instance.producer
        await asyncio.wait_for(producer.producer_pret().wait(), 30)

        reponse = await producer.executer_requete(
            dict(), 'CoreTopologie', 'getConsignationFichiers', exchange="2.prive")

        try:
            return reponse.parsed['consignation_url']
        except Exception as e:
            self.__logger.exception("Erreur chargement URL consignation")

    async def download_fichier(self, fuuid, path_destination):
        await self.ouvrir_sessions()  # S'assurer d'avoir une session ouverte
        url_fuuid = self.get_url_fuuid(fuuid)
        raise NotImplementedError('fix me')

    async def verifier_existance(self, fuuid: str) -> dict:
        """
        Requete HEAD pour verifier que le fichier existe sur la consignation locale.
        :param fuuid:
        :return:
        """
        await self.ouvrir_sessions()  # S'assurer d'avoir une session ouverte
        url_fuuid = self.get_url_fuuid(fuuid)
        reponse = await self.__session_http_requests.head(url_fuuid)
        return {'taille': reponse.headers.get('Content-Length'), 'status': reponse.status}

    def get_url_fuuid(self, fuuid):
        return f"{self.__url_consignation}/fichiers_transfert/{fuuid}"
