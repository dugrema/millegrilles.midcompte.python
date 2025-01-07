import aiohttp
import asyncio
import datetime
import json
import logging
import os
import pathlib
import multibase

from asyncio import TaskGroup
from typing import Optional, Union
from urllib.parse import urljoin

from millegrilles_messages.bus.BusContext import ForceTerminateExecution
from millegrilles_messages.messages import Constantes
from millegrilles_messages.chiffrage.DechiffrageUtils import get_decipher_cle_secrete
from millegrilles_messages.structs.Filehost import Filehost

from millegrilles_streaming import Constantes as StreamingConstants
from millegrilles_streaming.Context import StreamingContext
from millegrilles_streaming.Structs import InformationFuuid


LOGGER = logging.getLogger(__name__)

CONST_MAX_RETRIES_CLE = 3


class KeyRetrievalException(Exception):
    pass


class IntakeJob:

    def __init__(self, info: InformationFuuid, cle_dechiffree: Union[str, bytes]):
        self.__creation = datetime.datetime.now()
        self.__info = info
        if isinstance(cle_dechiffree, str):
            self.decrypted_key = multibase.decode('m' + cle_dechiffree)
        else:
            self.decrypted_key = cle_dechiffree

        self.status_code: Optional[int] = None
        self.file_size: Optional[int] = None        # Encrypted file size
        self.file_position: Optional[int] = None    # Current decryption position

        self.ready_event: Optional[asyncio.Event] = asyncio.Event()  # Set quand le download est pret

    @property
    def creation_date(self) -> datetime.datetime:
        return self.__creation

    @property
    def file_information(self) -> InformationFuuid:
        return self.__info

    @property
    def fuuid(self):
        return self.__info.fuuid


class IntakeHandler:

    def __init__(self, context: StreamingContext):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self._context = context
        self.__job_status: dict[str, IntakeJob] = dict()  # {fuuid: IntakeJob}

        # Create and hold on to a session. It has a cookie that lasts 1 hour.
        self.__session: Optional[aiohttp.ClientSession] = None

        self.__job_queue: asyncio.Queue[Optional[IntakeJob]] = asyncio.Queue()

        self.__loop = asyncio.get_event_loop()

    async def run(self):
        try:
            async with TaskGroup() as group:
                group.create_task(self.__run_jobs())
                group.create_task(self.__job_status_maintenance_thread())
                group.create_task(self.__stop_thread())
        except* Exception:
            if self._context.stopping is False:
                self.__logger.exception("Unhandled async thread exception")
                self._context.stop()
                raise ForceTerminateExecution()

    def clear_session(self):
        if self.__session is not None:
            self.__loop.call_soon_threadsafe(self.__session.close)
        self.__session = None

    async def __stop_thread(self):
        await self._context.wait()
        # Release threads
        await self.__job_queue.put(None)

    async def __job_status_maintenance_thread(self):
        while self._context.stopping is False:
            job_expiration = datetime.datetime.now() - datetime.timedelta(hours=1)
            expired_list = list()
            for fuuid, job in self.__job_status.items():
                if job.creation_date < job_expiration:
                    expired_list.append(fuuid)
            if len(expired_list) > 0:
                self.__logger.warning("__job_status_maintenance_thread Queue empty but %s download statuses are leftover - clearing", len(expired_list))
                for fuuid in expired_list:
                    try:
                        del self.__job_status[fuuid]
                    except KeyError:
                        pass  #
            await self._context.wait(900)

    async def __run_jobs(self):
        while self._context.stopping is False:
            job = await self.__job_queue.get()

            if job is None:
                return  # Exit condition

            try:
                await self.__run_job(job)
            except asyncio.CancelledError as e:
                raise e
            except:
                self.__logger.exception("Unhandled error processing job")
            finally:
                try:
                    del self.__job_status[job.fuuid]
                except KeyError:
                    pass  # Job gone

    async def __run_job(self, job: IntakeJob):
        # Downloader/dechiffrer
        fuuid = job.fuuid

        self.__logger.debug("Downloader %s" % fuuid)
        work_file_path = pathlib.Path(self._context.download_path, f'{fuuid}.dat.work')
        work_json_path = pathlib.Path(self._context.download_path, f'{fuuid}.json')
        decrypted_file_path = job.file_information.path_complet
        json_path = pathlib.Path(decrypted_file_path.parent, f'{fuuid}.json')

        if work_file_path.exists():
            return  # Nothing to do
        elif decrypted_file_path.exists():
            return  # Nothing to do

        # Save file information
        file_info_dict = {
            'fuuid': fuuid,
            'mimetype': job.file_information.mimetype,
            'file_size': job.file_size,
            'file_position': 0,
            # 'jwt_token': job.file_information.jwt_token  # Security issue, do not save
        }

        # Use the info file as a lock on the download. Will prevent other streaming processes from starting
        # the same download when the job is already being processed.
        with work_json_path.open(mode='wt') as fp:
            await asyncio.to_thread(json.dump, file_info_dict, fp)

            try:
                try:
                    # Download and decrypt file
                    with open(work_file_path, 'wb') as fp:
                        await self.__download_decipher_file(job, fp)
                    self.__logger.debug("File %s has been downloaded and decrypted", fuuid)
                    # Move file
                    work_file_path.rename(decrypted_file_path)
                    work_json_path.rename(json_path)
                except:
                    self.__logger.exception("Unhandled exception in download")
                    return
            finally:
                # Ensure cleanup
                job.ready_event.set()
                try:
                    del self.__job_status[fuuid]
                except KeyError:
                    pass  # Job already deleted
                work_file_path.unlink(missing_ok=True)
                work_json_path.unlink(missing_ok=True)

    async def __download_decipher_file(self, job: IntakeJob, tmp_file):
        fuuid = job.fuuid
        decrypted_key = job.decrypted_key

        for i in range(0, 3):
            self.__logger.info("Downloading/decrypting %s (attempt #%s)", fuuid, i)
            # Reload parameters, information may have changed on retry
            url_fichier = urljoin(self._context.filehost_url, f'filehost/files/{fuuid}')

            # Reset position when retrying, use new decipher from scratch (TODO: resume on connection reset)
            decryption_info = {'nonce': job.file_information.nonce, 'format': job.file_information.format, 'header': job.file_information.header}
            decipher = get_decipher_cle_secrete(decrypted_key, decryption_info)
            tmp_file.seek(0)
            job.file_position = 0  # Reset position for user feedback

            if self.__session is None:
                timeout = aiohttp.ClientTimeout(connect=5, total=900)
                self.__session = self._context.get_http_session(timeout)

            try:
                async with self.__session.get(url_fichier) as resp:
                    resp.raise_for_status()

                    async for chunk in resp.content.iter_chunked(64*1024):
                        await asyncio.to_thread(tmp_file.write, decipher.update(chunk))
                        job.file_position += len(chunk)  # User feedback
                    await asyncio.to_thread(tmp_file.write, decipher.finalize())

                    break  # Download successful, no retry
            except aiohttp.ClientResponseError as e:
                if e.status == 401:
                    await filehost_authenticate(self._context, self.__session)
                    # Will retry
                elif 500 <= e.status < 600:
                    # Server error, wait a while until retry
                    await self._context.wait(5)
                else:
                    raise e
                await self._context.wait(1)  # Cool down before retry

    async def get_fuuid_header(self, fuuid: str, session: aiohttp.ClientSession) -> dict:
        """
        Requete HEAD pour verifier que le fichier existe sur la consignation locale.
        :param fuuid:
        :param session:
        :return:
        """
        file_url = urljoin(self._context.filehost_url, f'filehost/files/{fuuid}')
        response = await session.head(file_url)
        if response.status == 401:
            # Cookie expired, try again
            await filehost_authenticate(self._context, session)
            response = await session.head(file_url)
        response.raise_for_status()
        return {'taille': response.headers.get('Content-Length'), 'status': response.status}

    async def __get_key(self, user_id: str, fuuid: str, jwt_token: str, timeout=15) -> bytes:
        producer = await self._context.get_producer()
        domaine = 'GrosFichiers'
        action = 'getClesStream'
        key_request = {'user_id': user_id, 'fuuids': [fuuid], 'jwt': jwt_token}
        key_response = await producer.request(
            key_request,
            domain=domaine, action=action, exchange='2.prive', timeout=timeout, domain_check=['GrosFichiers', 'MaitreDesCles'])
        reponse_parsed = key_response.parsed

        if reponse_parsed.get('ok') is not True:
            raise Exception('Key access refused: %s' % reponse_parsed['err'])

        # Retrieve the key
        keys = reponse_parsed['cles']
        if len(keys) == 1:
            key_response = keys[0]
        else:
            raise Exception('Number of received key != 1')

        decrypted_key: bytes = multibase.decode('m'+key_response['cle_secrete_base64'])
        return decrypted_key

    async def add_job(self, file: InformationFuuid) -> IntakeJob:
        try:
            existing_job = self.__job_status[file.fuuid]
        except KeyError:
            pass  # New job
        else:
            try:
                # Wait a while to see if job finishes
                await asyncio.wait_for(existing_job.ready_event.wait(), 3)
                return existing_job  # Job done
            except asyncio.TimeoutError:
                # Job did not finish, return current progress
                return existing_job

        # Check if the decrypted file is already available
        try:
            await self.load_decrypted_information(file)
            raise FileExistsError()
        except FileNotFoundError:
            pass  # File does not exist

        # Check that file exists on filehost
        if self.__session is None:
            timeout = aiohttp.ClientTimeout(connect=5, total=900)
            self.__session = self._context.get_http_session(timeout)
        file_header = await self.get_fuuid_header(file.fuuid, self.__session)

        # Get decryption key
        decrypted_key = await self.__get_key(file.user_id, file.fuuid, file.jwt_token)

        # Create job and enqueue
        job = IntakeJob(file, decrypted_key)
        job.file_size = file_header['taille']
        # Set job values from file_header
        self.__job_status[file.fuuid] = job
        await self.__job_queue.put(job)

        try:
            # Wait for a few seconds in case the job can complete quickly
            await asyncio.wait_for(job.ready_event.wait(), 3)
        except asyncio.TimeoutError:
            pass

        return job  # Return the same job, contains updated status

    async def load_decrypted_information(self, file_information: InformationFuuid):
        fuuid = file_information.fuuid
        decrypted_file_path = pathlib.Path(self._context.decrypted_path, f'{fuuid}.dat')
        json_path = pathlib.Path(self._context.decrypted_path, f'{fuuid}.json')

        # Assigned the full path here even if file is not yet available
        file_information.path_complet = decrypted_file_path

        if await asyncio.to_thread(decrypted_file_path.exists):
            with open(json_path, 'rt') as fp:
                info = await asyncio.to_thread(json.load, fp)
            file_information.set_params(info)
        else:
            raise FileNotFoundError()


async def filehost_authenticate(context: StreamingContext, session: aiohttp.ClientSession):
    filehost_url = context.filehost_url
    url_authenticate = urljoin(filehost_url, '/filehost/authenticate')
    authentication_message, message_id = context.formatteur.signer_message(
        Constantes.KIND_COMMANDE, dict(), domaine='filehost', action='authenticate')
    authentication_message['millegrille'] = context.formatteur.enveloppe_ca.certificat_pem
    async with session.post(url_authenticate, json=authentication_message) as resp:
        resp.raise_for_status()

# class IntakeStreaming(IntakeHandler):
#     """
#     Gere le dechiffrage des videos.
#     """
#
#     def __init__(self, stop_event: asyncio.Event, etat_instance: EtatInstance,
#                  consignation_handler: ConsignationHandler, timeout_cycle: Optional[int] = None):
#         super().__init__(stop_event, etat_instance, timeout_cycle)
#         self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
#         self.__consignation_handler = consignation_handler
#
#         self.__jobs: Optional[asyncio.Queue[IntakeJob]] = None
#
#         self.__events_fuuids = dict()
#
#     async def run(self):
#         async with TaskGroup() as group:
#             group.create_task(super().run())
#             group.create_task(self.entretien_dechiffre_thread())
#             group.create_task(self.entretien_download_thread())
#
#
#     async def configurer(self):
#         self.__jobs = asyncio.Queue(maxsize=5)
#         return await super().configurer()
#
#     async def entretien_download_thread(self):
#         while self._stop_event.is_set() is False:
#             self.__logger.debug("Entretien download")
#             path_download = pathlib.Path(self.get_path_download())
#             fuuids_supprimes = entretien_download(path_download, self.__events_fuuids)
#             try:
#                 await asyncio.wait_for(self._stop_event.wait(), timeout=300)
#             except asyncio.TimeoutError:
#                 pass
#
#     async def entretien_dechiffre_thread(self):
#         while self._stop_event.is_set() is False:
#             self.__logger.debug("Entretien dechiffre")
#             path_dechiffre = pathlib.Path(self.get_path_dechiffre())
#             fuuids_supprimes = entretien_dechiffre(path_dechiffre)
#             try:
#                 await asyncio.wait_for(self._stop_event.wait(), timeout=300)
#             except asyncio.TimeoutError:
#                 pass
#
#     async def traiter_prochaine_job(self) -> Optional[dict]:
#         try:
#             job = self.__jobs.get_nowait()
#             fuuid = job.fuuid
#             self.__logger.debug("Traiter job streaming pour fuuid %s" % fuuid)
#             await self.traiter_job(job)
#         except asyncio.QueueEmpty:
#             return None  # Condition d'arret de l'intake
#         except Exception as e:
#             self.__logger.exception("Erreur traitement job download")
#             return {'ok': False, 'err': str(e)}
#
#         return {'ok': True}
#
#     async def annuler_job(self, job: dict, emettre_evenement=False):
#         raise NotImplementedError('must override')
#
#     async def traiter_job(self, job):
#         fuuid = job.fuuid
#
#         path_download_fichier = pathlib.Path(self.get_path_download(), fuuid + '.work')
#         path_download_json = pathlib.Path(self.get_path_download(), fuuid + '.json')
#
#         params_dechiffrage = job.info.get_params_dechiffrage()
#         await self.__consignation_handler.download_fichier(fuuid, job.cle_dechiffree, params_dechiffrage, path_download_fichier)
#
#         # Download reussi, deplacer les fichiers vers repertoire dechiffre
#         path_dechiffre_fichier = pathlib.Path(self.get_path_dechiffre(), fuuid + '.dat')
#         path_dechiffre_json = pathlib.Path(self.get_path_dechiffre(), fuuid + '.json')
#
#         os.rename(path_download_fichier, path_dechiffre_fichier)
#         os.rename(path_download_json, path_dechiffre_json)
#
#         try:
#             event_download = self.__events_fuuids[fuuid]
#             event_download.set()
#             del self.__events_fuuids[fuuid]
#         except KeyError:
#             pass  # Ok
#
#     def cleanup_download(self, fuuid):
#         try:
#             event_download = self.__events_fuuids[fuuid]
#             del self.__events_fuuids[fuuid]
#             event_download.set()
#         except (AttributeError, KeyError) as e2:
#             self.__logger.info("cleanup_download Erreur del info download %s en memoire (%s)" % (fuuid, e2))
#
#     async def __ajouter_job(self, info: InformationFuuid):
#         """
#         :param info: Fuuid a downloader et dechiffrer.
#         :return:
#         :raises asyncio.QueueFull: Si q de jobs est pleine.
#         """
#         fuuid = info.fuuid
#
#         try:
#             self.__events_fuuids[fuuid]
#         except KeyError:
#             pass  # Ok, la job n'existe pas en memoire
#         else:
#             raise Exception("La job sur fuuid %s existe deja" % fuuid)
#
#         try:
#             # Creer evenement d'attente pour metter les autres requetes en attente sur ce process
#             self.__events_fuuids[fuuid] = asyncio.Event()
#
#             path_download_json = pathlib.Path(os.path.join(self.get_path_download(), fuuid + '.json'))
#
#             # Verifier que le fichier existe sur la consignation (requete HEAD)
#             reponse_head = await self.__consignation_handler.verifier_existance(fuuid)
#             status_fuuid = reponse_head['status']
#             info_fichier = {
#                 'fuuid': fuuid,
#                 'mimetype': info.mimetype,
#                 'status': status_fuuid,
#                 'taille': reponse_head['taille'],
#                 'jwt_token': info.jwt_token
#             }
#             with path_download_json.open(mode='w') as fichier:
#                 json.dump(info_fichier, fichier)
#
#             if status_fuuid != 200:
#                 # Le fichier n'est pas disponible. Plus rien a faire
#                 self.__logger.debug('__ajouter_job Fichier %s non disponible sur consignation' % fuuid)
#                 self.cleanup_download(fuuid)
#                 return info_fichier
#         except Exception as e:
#             self.__logger.exception("__ajouter_job Erreur verification existance fichier %s" % fuuid)
#             self.cleanup_download(fuuid)
#             raise e
#
#         try:
#             # Recuperer la cle pour dechiffrer la job
#             ref_fuuid = info.ref or info.fuuid
#             reponse_cle = None
#             for i in range(1, CONST_MAX_RETRIES_CLE+1):
#                 self.__logger.debug("__ajouter_job Recuperer_cle (try %d)" % i)
#                 try:
#                     reponse_cle = await self.recuperer_cle(info.user_id, ref_fuuid, info.jwt_token, timeout=30)
#                     break
#                 except (asyncio.CancelledError, asyncio.TimeoutError) as e:
#                     self.__logger.warning("__ajouter_job Timeout recuperer_cle (try %d de %d)" % (i, CONST_MAX_RETRIES_CLE))
#                     if i == CONST_MAX_RETRIES_CLE:
#                         raise e
#
#             cle_chiffree = reponse_cle['cle_secrete_base64']
#
#             if info.format is None:
#                 # On travaille avec le fichier original, copier info chiffrage
#                 info.format = reponse_cle['format']
#                 info.header = reponse_cle.get('header')
#                 info.nonce = reponse_cle.get('nonce')
#
#             self.__logger.debug('__ajouter_job Creer la job de download pour fuuid %s' % fuuid)
#             job = IntakeJob(info, cle_chiffree)
#             self.__jobs.put_nowait(job)
#             # S'assurer de demarrer le traitement immediatement
#             await self.trigger_traitement()
#         except Exception as e:
#             # Set event attent et supprimer
#             self.cleanup_download(fuuid)
#
#             # Cleanup du json, abort le download
#             path_download_json.unlink(missing_ok=True)
#
#             raise e
#
#         return info_fichier
#
#     async def recuperer_cle(self, user_id: str, fuuid: str, jwt_token: str, timeout=15) -> dict:
#         producer = self._etat_instance.producer
#         await asyncio.wait_for(producer.producer_pret().wait(), 1)
#
#         domaine = 'GrosFichiers'
#         action = 'getClesStream'
#         requete_cle = {'user_id': user_id, 'fuuids': [fuuid], 'jwt': jwt_token}
#         reponse_cle = await producer.executer_requete(
#             requete_cle,
#             domaine=domaine, action=action, exchange='2.prive', timeout=timeout)
#         reponse_parsed = reponse_cle.parsed
#
#         if reponse_parsed.get('ok') is not True:
#             raise Exception('acces cle refuse : %s' % reponse_parsed['err'])
#
#         # reponse_cle = reponse_parsed['cles'][fuuid]
#         cles = reponse_parsed['cles']
#         if len(cles) == 1:
#             reponse_cle = cles[0]
#         else:
#             raise Exception('recuperer_cle Le nombre de cles recues != 1')
#
#         # Test pour voir si la cle est dechiffrable
#         # clecertificat = self._etat_instance.clecertificat
#         # cle_chiffree = reponse_cle['cle']
#         # _cle_dechiffree = clecertificat.dechiffrage_asymmetrique(cle_chiffree)
#
#         return reponse_cle
#
#     def get_fichier_dechiffre(self, fuuid) -> Optional[InformationFuuid]:
#         """
#         :param fuuid: Fuuid du fichier dechiffre.
#         :return: L'information pour acceder au fichier dechiffre, incluant metadonnes. None si fichier n'existe pas.
#         """
#         path_dechiffre_dat = pathlib.Path(self.get_path_dechiffre(), fuuid + '.dat')
#
#         try:
#             stat_dat = path_dechiffre_dat.stat()
#         except FileNotFoundError:
#             return None
#
#         # Touch le fichier pour indiquer qu'on l'utilise encore
#         path_dechiffre_dat.touch()
#
#         # Charger les metadonnees (json associe)
#         path_dechiffre_json = pathlib.Path(os.path.join(self.get_path_dechiffre(), fuuid + '.json'))
#         with path_dechiffre_json.open() as fichier:
#             info_json = json.load(fichier)
#
#         info = InformationFuuid(fuuid, None, info_json)
#         info.path_complet = str(path_dechiffre_dat)
#         info.taille = stat_dat.st_size
#
#         return info
#
#     def get_progres_download(self, fuuid) -> Optional[InformationFuuid]:
#         path_download_json = pathlib.Path(os.path.join(self.get_path_download(), fuuid + '.json'))
#
#         if path_download_json.exists() is False:
#             # Le fichier n'est pas en download / traitement
#             return None
#
#         with path_download_json.open() as fichier:
#             contenu_json = json.load(fichier)
#
#         path_fichier_work = pathlib.Path(os.path.join(self.get_path_download(), fuuid + '.work'))
#         try:
#             stat_dat = path_fichier_work.stat()
#         except FileNotFoundError:
#             # On n'a pas de fichier .work. Retourner le contenu du .json (peut avoir un status d'erreur, e.g. 404).
#             reponse = InformationFuuid(fuuid, None, contenu_json)
#             reponse.position_courante = 0
#             return reponse
#
#         # Retourner l'information du fichier avec taille totale et position courante
#         reponse = InformationFuuid(fuuid, None, contenu_json)
#         reponse.position_courante = stat_dat.st_size
#
#         return reponse
#
#     def get_path_dechiffre(self) -> pathlib.Path:
#         path_staging = self._etat_instance.configuration.dir_staging
#         return pathlib.Path(path_staging, Constantes.DIR_DECHIFFRE)
#
#     def get_path_download(self) -> pathlib.Path:
#         path_staging = self._etat_instance.configuration.dir_staging
#         return pathlib.Path(path_staging, Constantes.DIR_DOWNLOAD)
#
#     async def attendre_download(self, fuuid: str, jwt_token: str, params: dict, timeout: Optional[int] = None) -> Optional[InformationFuuid]:
#         # Verifier si le fichier est deja dechiffre
#         info = self.get_fichier_dechiffre(fuuid)
#         if info is not None:
#             return info
#
#         event_attente = None
#
#         # Verifier si le download existe deja
#         try:
#             event_attente = self.__events_fuuids[fuuid]
#         except KeyError:
#             info = self.get_progres_download(fuuid)
#             if info is None:
#                 timeout = None  # Ne pas attendre pour retourner l'information
#
#                 # Creer la job de download
#                 info = InformationFuuid(fuuid, jwt_token, params)
#                 await info.init()
#                 reponse = await self.__ajouter_job(info)
#
#                 if reponse['status'] != 200:
#                     info.status = reponse['status']
#                     return info
#
#         if event_attente is not None and timeout is not None:
#             try:
#                 await asyncio.wait_for(event_attente.wait(), timeout)
#             except asyncio.TimeoutError:
#                 pass
#             if self._stop_event.is_set():
#                 raise Exception('thread stopped')
#         elif timeout is not None:
#             try:
#                 await asyncio.wait_for(self._stop_event.wait(), timeout)
#             except asyncio.TimeoutError:
#                 pass  # OK
#
#         # Tenter de charger le fichier. Retourner
#         info = self.get_fichier_dechiffre(fuuid)
#         if info is not None:
#             return info
#
#         # On n'a aucune information, le download a peut-etre echoue (thread differente)
#         # Tenter de recharger l'information et retourner le resultat final (e.g. None)
#         return self.get_progres_download(fuuid)
#

