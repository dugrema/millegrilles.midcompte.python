import asyncio
import datetime
import json
import logging
import os
import pathlib

from typing import Optional

from millegrilles_fichiers import Constantes
from millegrilles_messages.jobs.Intake import IntakeHandler
from millegrilles_messages.MilleGrillesConnecteur import EtatInstance
from millegrilles_fichiers.Consignation import ConsignationHandler, InformationFuuid


LOGGER = logging.getLogger(__name__)

# CONST_MAX_RETRIES_CLE = 2


class IntakeJob:

    def __init__(self, info: InformationFuuid, cle_chiffree):
        self.info = info
        self.cle_chiffree = cle_chiffree

    @property
    def fuuid(self):
        return self.info.fuuid


class IntakeStreaming(IntakeHandler):
    """
    Gere le dechiffrage des videos.
    """

    def __init__(self, stop_event: asyncio.Event, etat_instance: EtatInstance,
                 consignation_handler: ConsignationHandler, timeout_cycle: Optional[int] = None):
        super().__init__(stop_event, etat_instance, timeout_cycle)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__consignation_handler = consignation_handler

        self.__jobs: Optional[asyncio.Queue[IntakeJob]] = None

        self.__events_fuuids = dict()

    async def run(self):
        await asyncio.gather(
            super().run(),
            # self.entretien_dechiffre_thread(),
            # self.entretien_download_thread()
        )

    async def configurer(self):
        self.__jobs = asyncio.Queue(maxsize=5)
        return await super().configurer()

    # async def entretien_download_thread(self):
    #     wait_coro = self._stop_event.wait()
    #     while self._stop_event.is_set() is False:
    #         self.__logger.debug("Entretien download")
    #         path_download = pathlib.Path(self.get_path_download())
    #         fuuids_supprimes = entretien_download(path_download, self.__events_fuuids)
    #         await asyncio.wait([wait_coro], timeout=20)

    # async def entretien_dechiffre_thread(self):
    #     wait_coro = self._stop_event.wait()
    #     while self._stop_event.is_set() is False:
    #         self.__logger.debug("Entretien dechiffre")
    #         path_dechiffre = pathlib.Path(self.get_path_dechiffre())
    #         fuuids_supprimes = entretien_dechiffre(path_dechiffre)
    #         await asyncio.wait([wait_coro], timeout=300)

    async def traiter_prochaine_job(self) -> Optional[dict]:
        try:
            job = self.__jobs.get_nowait()
            fuuid = job.fuuid
            self.__logger.debug("Traiter job streaming pour fuuid %s" % fuuid)
            await self.traiter_job(job)
        except asyncio.QueueEmpty:
            return None  # Condition d'arret de l'intake
        except Exception as e:
            self.__logger.exception("Erreur traitement job download")
            return {'ok': False, 'err': str(e)}

        return {'ok': True}

    async def annuler_job(self, job: dict, emettre_evenement=False):
        raise NotImplementedError('must override')

    async def traiter_job(self, job):
        fuuid = job.fuuid
        raise NotImplementedError('must override')

    # def cleanup_download(self, fuuid):
    #     try:
    #         event_download = self.__events_fuuids[fuuid]
    #         del self.__events_fuuids[fuuid]
    #         event_download.set()
    #     except (AttributeError, KeyError) as e2:
    #         self.__logger.info("cleanup_download Erreur del info download %s en memoire (%s)" % (fuuid, e2))

    # async def __ajouter_job(self, info: InformationFuuid):
    #     """
    #     :param info: Fuuid a downloader et dechiffrer.
    #     :return:
    #     :raises asyncio.QueueFull: Si q de jobs est pleine.
    #     """
    #     fuuid = info.fuuid
    #
    #     try:
    #         self.__events_fuuids[fuuid]
    #     except KeyError:
    #         pass  # Ok, la job n'existe pas en memoire
    #     else:
    #         raise Exception("La job sur fuuid %s existe deja" % fuuid)
    #
    #     try:
    #         # Creer evenement d'attente pour metter les autres requetes en attente sur ce process
    #         self.__events_fuuids[fuuid] = asyncio.Event()
    #
    #         path_download_json = pathlib.Path(os.path.join(self.get_path_download(), fuuid + '.json'))
    #
    #         # Verifier que le fichier existe sur la consignation (requete HEAD)
    #         reponse_head = await self.__consignation_handler.verifier_existance(fuuid)
    #         status_fuuid = reponse_head['status']
    #         info_fichier = {
    #             'fuuid': fuuid,
    #             'mimetype': info.mimetype,
    #             'status': status_fuuid,
    #             'taille': reponse_head['taille'],
    #             'jwt_token': info.jwt_token
    #         }
    #         with path_download_json.open(mode='w') as fichier:
    #             json.dump(info_fichier, fichier)
    #
    #         if status_fuuid != 200:
    #             # Le fichier n'est pas disponible. Plus rien a faire
    #             self.__logger.debug('Fichier %s non disponible sur consignation' % fuuid)
    #             self.cleanup_download(fuuid)
    #             return info_fichier
    #     except Exception as e:
    #         self.__logger.exception("Erreur verification existance fichier %s" % fuuid)
    #         self.cleanup_download(fuuid)
    #         raise e
    #
    #     try:
    #         # Recuperer la cle pour dechiffrer la job
    #         ref_fuuid = info.ref or info.fuuid
    #         reponse_cle = None
    #         for i in range(1, CONST_MAX_RETRIES_CLE+1):
    #             self.__logger.debug("Recuperer_cle (try %d)" % i)
    #             try:
    #                 reponse_cle = await self.recuperer_cle(info.user_id, ref_fuuid, info.jwt_token, timeout=6)
    #                 break
    #             except (asyncio.CancelledError, asyncio.TimeoutError) as e:
    #                 self.__logger.warning("Timeout recuperer_cle (try %d de %d)" % (i, CONST_MAX_RETRIES_CLE))
    #                 if i == CONST_MAX_RETRIES_CLE:
    #                     raise e
    #
    #         cle_chiffree = reponse_cle['cle']
    #
    #         if info.format is None:
    #             # On travaille avec le fichier original, copier info chiffrage
    #             info.format = reponse_cle['format']
    #             info.header = reponse_cle.get('header')
    #
    #         self.__logger.debug('Creer la job de download pour fuuid %s' % fuuid)
    #         job = IntakeJob(info, cle_chiffree)
    #         self.__jobs.put_nowait(job)
    #         # S'assurer de demarrer le traitement immediatement
    #         await self.trigger_traitement()
    #     except Exception as e:
    #         # Set event attent et supprimer
    #         self.cleanup_download(fuuid)
    #
    #         # Cleanup du json, abort le download
    #         path_download_json.unlink(missing_ok=True)
    #
    #         raise e
    #
    #     return info_fichier


# def entretien_download(path_download: pathlib.Path, dict_attente: dict, timeout=Constantes.CONST_TIMEOUT_DOWNLOAD) -> list:
#     dt_expiration = datetime.datetime.now() - datetime.timedelta(seconds=timeout)
#     ts_expiration = dt_expiration.timestamp()
#
#     fuuids_supprimes = list()
#
#     # Supprimer les fichiers .dat (et .json associe)
#     for fichier in path_download.iterdir():
#         if fichier.match('*.work'):
#             fuuid = fichier.name.split('.')[0]
#             LOGGER.debug("Verifier expiration fichier download %s" % str(fichier))
#             stat_fichier = fichier.stat()
#             if stat_fichier.st_mtime < ts_expiration:
#                 LOGGER.debug("Download %s est expire, on le supprime" % fuuid)
#
#                 try:
#                     dict_attente[fuuid].set()
#                     del dict_attente[fuuid]
#                 except KeyError:
#                     pass  # OK
#
#                 fichier.unlink()
#
#                 fichier_json = pathlib.Path(str(fichier).replace('.work', '.json'))
#                 try:
#                     fichier_json.unlink()
#                 except FileNotFoundError:
#                     pass  # Ok
#                 fuuids_supprimes.append(fuuid)
#
#     # Cleanup des .json expires et orphelins
#     for fichier in path_download.iterdir():
#         if fichier.match('*.json'):
#             fuuid = fichier.name.split('.')[0]
#             stat_json = fichier.stat()
#             if stat_json.st_mtime < ts_expiration:
#                 # Le json est vieux - verifier s'il existe un fichier .work associe
#                 fichier_work = pathlib.Path(str(fichier).replace('.json', '.work'))
#                 if fichier_work.exists() is False:
#                     try:
#                         dict_attente[fuuid].set()
#                         del dict_attente[fuuid]
#                     except KeyError:
#                         pass  # OK
#
#                     # Le fichier json n'a aucun .dat associe, on supprime
#                     LOGGER.warning("Supprimer fichier json orphelin %s" % str(fichier))
#                     fichier.unlink()
#                     fuuids_supprimes.append(fuuid)
#
#     return fuuids_supprimes


# def entretien_dechiffre(path_dechiffre: pathlib.Path, timeout=Constantes.CONST_TIMEOUT_DECHIFFRE):
#     pass
#
#     """
#     Supprime les fichiers dechiffres qui ont ete supprimes.
#     :param path_dechiffre:
#     :param timeout:
#     :return: Liste de fuuids qui ont ete supprimes
#     """
#
#     dt_expiration = datetime.datetime.now() - datetime.timedelta(seconds=timeout)
#     ts_expiration = dt_expiration.timestamp()
#
#     fuuids_supprimes = list()
#
#     # Supprimer les fichiers .dat (et .json associe)
#     for fichier in path_dechiffre.iterdir():
#         if fichier.match('*.dat'):
#             LOGGER.debug("Verifier expiration fichier dechiffre %s" % str(fichier))
#             stat_fichier = fichier.stat()
#             if stat_fichier.st_mtime < ts_expiration:
#                 # Fichier expire
#                 LOGGER.debug("Fichier dechiffre est expire %s" % str(fichier))
#                 fuuid = fichier.name
#                 fichier.unlink()
#                 fichier_json = pathlib.Path(str(fichier).replace('.dat', '.json'))
#                 try:
#                     fichier_json.unlink()
#                 except FileNotFoundError:
#                     pass  # Ok
#                 fuuids_supprimes.append(fuuid)
#
#     # Cleanup des .json orphelins
#     for fichier in path_dechiffre.iterdir():
#         if fichier.match('*.json'):
#             fichier_dat = pathlib.Path(str(fichier).replace('.json', '.dat'))
#             if fichier_dat.exists() is False:
#                 LOGGER.warning("Supprimer fichier json orphelin %s" % str(fichier))
#                 # Le fichier json n'a aucun .dat associe, on supprime
#                 fichier.unlink()
#
#     return fuuids_supprimes
