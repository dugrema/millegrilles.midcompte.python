import shutil

import aiohttp
import asyncio
import logging
import json
import os

from typing import Optional

from millegrilles_messages.messages import Constantes as ConstantesMillegrilles
from millegrilles_messages.chiffrage.DechiffrageUtils import get_decipher
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

    async def run(self):
        self.__logger.info("Demarrage run")
        await asyncio.gather(self.entretien())
        self.__logger.info("Fin run")

    async def entretien(self):
        stop_event_wait = self.__stop_event.wait()

        # Attendre 5 secondes avant le premier entretien
        #await asyncio.wait([stop_event_wait], timeout=5)

        while self.__stop_event.is_set() is False:
            await self.charger_consignation_url()
            await asyncio.wait([stop_event_wait], timeout=300)

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
            consignation_url = reponse.parsed['consignation_url']
            self.__url_consignation = consignation_url
            return consignation_url
        except Exception as e:
            self.__logger.exception("Erreur chargement URL consignation")

    async def download_fichier(self, fuuid, cle_chiffree, params_dechiffrage, path_destination):
        await self.ouvrir_sessions()  # S'assurer d'avoir une session ouverte
        url_fuuid = self.get_url_fuuid(fuuid)

        clecert = self.__etat_instance.clecertificat
        decipher = get_decipher(clecert, cle_chiffree, params_dechiffrage)

        timeout = aiohttp.ClientTimeout(connect=5, total=600)
        with path_destination.open(mode='wb') as output_file:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url_fuuid, ssl=self.__etat_instance.ssl_context) as resp:
                    resp.raise_for_status()

                    async for chunk in resp.content.iter_chunked(64*1024):
                        output_file.write(decipher.update(chunk))

            output_file.write(decipher.finalize())

    async def verifier_existance(self, fuuid: str) -> dict:
        """
        Requete HEAD pour verifier que le fichier existe sur la consignation locale.
        :param fuuid:
        :return:
        """
        await self.ouvrir_sessions()  # S'assurer d'avoir une session ouverte
        url_fuuid = self.get_url_fuuid(fuuid)
        reponse = await self.__session_http_requests.head(url_fuuid, ssl=self.__etat_instance.ssl_context)
        return {'taille': reponse.headers.get('Content-Length'), 'status': reponse.status}

    def get_url_fuuid(self, fuuid):
        return f"{self.__url_consignation}/fichiers_transfert/{fuuid}"
