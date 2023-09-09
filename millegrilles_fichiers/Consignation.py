import enum
import pathlib
import shutil

import aiohttp
import asyncio
import logging
import json
import os

from aiohttp import web
from typing import Optional

from millegrilles_messages.messages import Constantes as ConstantesMillegrilles
from millegrilles_fichiers import Constantes
from millegrilles_messages.chiffrage.DechiffrageUtils import dechiffrer_document
from millegrilles_messages.chiffrage.DechiffrageUtils import get_decipher
from millegrilles_fichiers.EtatFichiers import EtatFichiers
from millegrilles_fichiers.ConsignationStore import ConsignationStore, map_type


class InformationFuuid:

    def __init__(self, fuuid: str):
        self.fuuid = fuuid
        self.taille: Optional[int] = None               # Taille du fichier
        self.path_complet: Optional[str] = None         # Path complet sur disque du fichier dechiffre

        # if params is not None:
        #     self.set_params(params)

    @staticmethod
    def resolve_fuuid(etat_fichiers: EtatFichiers, fuuid: str):
        """ Trouver le path local du fichier par son fuuid. """
        info = InformationFuuid(fuuid)
        return info

    # def set_params(self, params: dict):
    #     self.taille = params.get('taille')
    #     self.status = params.get('status')
    #     self.user_id = params.get('userId') or params.get('user_id')


class ConsignationHandler:
    """
    Download et dechiffre les fichiers de media a partir d'un serveur de consignation
    """

    def __init__(self, stop_event: asyncio.Event, etat_instance: EtatFichiers):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__stop_event = stop_event
        self.__etat_instance = etat_instance

        self.__store_consignation: Optional[ConsignationStore] = None

        self.__url_consignation_primaire: Optional[str] = None

        self.__session_http_download: Optional[aiohttp.ClientSession] = None
        self.__session_http_requests: Optional[aiohttp.ClientSession] = None

        self.__est_primaire: Optional[bool] = None
        self.__store_pret_event: Optional[asyncio.Event] = None

    async def run(self):
        self.__logger.info("Demarrage run")

        self.__store_pret_event = asyncio.Event()

        await asyncio.gather(
            self.entretien(),
            self.thread_emettre_etat(),
        )

        self.__logger.info("Fin run")

    async def entretien(self):
        stop_event_wait = self.__stop_event.wait()

        while self.__stop_event.is_set() is False:
            try:
                await self.charger_topologie()
            except Exception:
                self.__logger.exception("Erreur charger_topologie")
            await asyncio.wait([stop_event_wait], timeout=300)

    async def thread_emettre_etat(self):
        stop_event_wait = self.__stop_event.wait()

        while self.__stop_event.is_set() is False:
            try:
                await self.emettre_etat()
            except Exception:
                self.__logger.exception("Erreur emettre_etat")
            await asyncio.wait([stop_event_wait], timeout=90)

    async def ouvrir_sessions(self):
        if self.__session_http_download is None or self.__session_http_download.closed:
            timeout = aiohttp.ClientTimeout(connect=5, total=300)
            self.__session_http_download = aiohttp.ClientSession(timeout=timeout)

        if self.__session_http_requests is None or self.__session_http_requests.closed:
            timeout_requests = aiohttp.ClientTimeout(connect=5, total=15)
            self.__session_http_requests = aiohttp.ClientSession(timeout=timeout_requests)

    async def modifier_topologie(self, configuration_topologie: dict):
        await self.__etat_instance.maj_topologie(configuration_topologie)

        producer = self.__etat_instance.producer
        await asyncio.wait_for(producer.producer_pret().wait(), 3)
        try:
            if self.__etat_instance.topologie.get('primaire') is True:
                # Faire un lien direct entre primaire et topologie (meme reference)
                self.__etat_instance.primaire = self.__etat_instance.topologie
            else:
                # Charger l'information sur la consignation primaire
                requete = {'primaire': True}
                reponse = await producer.executer_requete(
                    requete, 'CoreTopologie', 'getConsignationFichiers', exchange="2.prive")
                info_primaire = reponse.parsed
                if info_primaire['ok'] is True:
                    self.__etat_instance.primaire = info_primaire

        except Exception as e:
            self.__logger.exception("Erreur chargement consignation primaire")

        type_store = self.__etat_instance.topologie['type_store']
        class_type = map_type(type_store)
        if self.__store_consignation is None or self.__store_consignation.__class__ != class_type:
            self.__logger.info("Changer store consignation pour type %s" % type_store)
            instance_store = class_type(self.__etat_instance)
            self.__store_consignation = instance_store
            self.__store_consignation.initialiser_db()

        # La configuration du store est prete
        self.__store_pret_event.set()

    async def charger_topologie(self):
        # Indiquer qu'on rafrachi la topologie
        self.__store_pret_event.clear()

        """ Charge la configuration a partir de CoreTopologie """
        producer = self.__etat_instance.producer
        if producer is None:
            await asyncio.sleep(5)  # Attendre connexion MQ
            producer = self.__etat_instance.producer
            if producer is None:
                raise Exception('producer pas pret')
        await asyncio.wait_for(producer.producer_pret().wait(), 30)

        instance_id = self.__etat_instance.clecertificat.enveloppe.subject_common_name
        requete = {'instance_id': instance_id}
        reponse = await producer.executer_requete(
            requete, 'CoreTopologie', 'getConsignationFichiers', exchange="2.prive")

        if reponse.parsed['ok'] is True:
            # Conserver configuration topologie - dechiffrer partie chiffree
            configuration_topologie = reponse.parsed
            await self.modifier_topologie(configuration_topologie)
        else:
            # Aucune configuration connue pour l'instance
            # Mettre configuration par defaut et sauvegarder aupres de CoreTopologie
            await self.initialiser_nouvelle_consignation()

    async def emettre_etat(self):
        producer = self.__etat_instance.producer
        if producer is None or self.__etat_instance.topologie is None:
            await asyncio.sleep(5)  # Attendre connexion MQ, chargement de la configuration
            producer = self.__etat_instance.producer
            if producer is None:
                raise Exception('producer pas pret')
        await asyncio.wait_for(producer.producer_pret().wait(), 15)

        etat = {}
        configuration_topologie = self.__etat_instance.topologie
        for champ in Constantes.CONST_CHAMPS_CONFIG:
            try:
                val = configuration_topologie[champ]
                etat[champ] = val
            except KeyError:
                pass

        # Charger etat des fichiers (taille totale par type)
        if self.__store_consignation is not None:
            stats = self.__store_consignation.get_stats()
            etat.update(stats)

        # Charger etat downloads et uploads si secondaire

        await self.__etat_instance.producer.emettre_evenement(
            etat, Constantes.DOMAINE_FICHIERS, Constantes.EVENEMENT_PRESENCE,
            exchanges=ConstantesMillegrilles.SECURITE_PRIVE)

    async def initialiser_nouvelle_consignation(self):
        configuration_topologie_defaut = {
            'type_store': 'millegrille',
            'consignation_url': 'https://fichiers:443',
        }
        await self.modifier_topologie(configuration_topologie_defaut)

        await self.emettre_etat()

    async def consigner(self, path_source: pathlib.Path, fuuid: str):
        if self.__store_consignation is None:
            raise Exception("Store non initialise")
        await self.__store_consignation.consigner(path_source, fuuid)

    async def get_info_fichier(self, fuuid: str):
        if self.__store_consignation is None:
            raise Exception("Store non initialise")
        return self.__store_consignation.get_info_fichier(fuuid)

    async def stream_fuuid(self, fuuid: str, response: web.StreamResponse, start: Optional[int] = None, end: Optional[int] = None):
        if self.__store_consignation is None:
            raise Exception("Store non initialise")
        return await self.__store_consignation.stream_fuuid(fuuid, response, start, end)

    # async def charger_consignation_primaire(self):
    #     producer = self.__etat_instance.producer
    #     if producer is None:
    #         await asyncio.sleep(5)  # Attendre connexion MQ
    #         producer = self.__etat_instance.producer
    #         if producer is None:
    #             raise Exception('producer pas pret')
    #     await asyncio.wait_for(producer.producer_pret().wait(), 30)
    #
    #     reponse = await producer.executer_requete(
    #         dict(), 'CoreTopologie', 'getConsignationFichiers', exchange="2.prive")
    #
    #     try:
    #         consignation_url = reponse.parsed['consignation_url']
    #         self.__url_consignation_primaire = consignation_url
    #         return consignation_url
    #     except Exception as e:
    #         self.__logger.exception("Erreur chargement URL consignation")

    # async def download_fichier(self, fuuid, cle_chiffree, params_dechiffrage, path_destination):
    #     await self.ouvrir_sessions()  # S'assurer d'avoir une session ouverte
    #     url_fuuid = self.get_url_fuuid(fuuid)
    #
    #     clecert = self.__etat_instance.clecertificat
    #     decipher = get_decipher(clecert, cle_chiffree, params_dechiffrage)
    #
    #     timeout = aiohttp.ClientTimeout(connect=5, total=600)
    #     with path_destination.open(mode='wb') as output_file:
    #         async with aiohttp.ClientSession(timeout=timeout) as session:
    #             async with session.get(url_fuuid, ssl=self.__etat_instance.ssl_context) as resp:
    #                 resp.raise_for_status()
    #
    #                 async for chunk in resp.content.iter_chunked(64*1024):
    #                     output_file.write(decipher.update(chunk))
    #
    #         output_file.write(decipher.finalize())
    #
    # async def verifier_existance(self, fuuid: str) -> dict:
    #     """
    #     Requete HEAD pour verifier que le fichier existe sur la consignation locale.
    #     :param fuuid:
    #     :return:
    #     """
    #     await self.ouvrir_sessions()  # S'assurer d'avoir une session ouverte
    #     url_fuuid = self.get_url_fuuid(fuuid)
    #     reponse = await self.__session_http_requests.head(url_fuuid, ssl=self.__etat_instance.ssl_context)
    #     return {'taille': reponse.headers.get('Content-Length'), 'status': reponse.status}
    #
    # def get_url_fuuid(self, fuuid):
    #     return f"{self.__url_consignation}/fichiers_transfert/{fuuid}"
