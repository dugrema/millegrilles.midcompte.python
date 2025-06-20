import asyncio
from urllib.parse import urljoin

import aiohttp
import logging
import tempfile
import multibase

from typing import Optional, TypedDict, Union
from ssl import SSLContext

from asyncio import Event, TaskGroup

from millegrilles_media.Structs import VersionJob, SecretKeyDict
from millegrilles_messages.messages import Constantes
from millegrilles_media.Context import MediaContext
from millegrilles_media.TransfertFichiers import filehost_authenticate
from millegrilles_messages.chiffrage.DechiffrageUtils import get_decipher_cle_secrete
from millegrilles_messages.Mimetypes import est_video
from millegrilles_media.ImagesHandler import traiter_image
from millegrilles_media.VideosHandler import VideoConversionJob
from millegrilles_messages.messages.MessagesModule import MessageWrapper


class IntakeHandler:

    def __init__(self, context: MediaContext):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self._context = context
        self.__event_fichiers: Event = None
        self.__ssl_context: Optional[SSLContext] = None

        self.__session: Optional[aiohttp.ClientSession] = None

    async def configurer(self):
        self.__event_fichiers = Event()

        config = self._context.configuration
        cert_path = config.cert_pem_path
        self.__ssl_context = SSLContext()
        self.__ssl_context.load_cert_chain(cert_path, config.key_pem_path)

    def get_job_type(self) -> str:
        raise NotImplementedError('must implement')

    async def run(self):
        await self._context.wait()

    async def process_job(self, job: dict):
        try:
            decrypted_key: bytes = await self._get_key(job)
            job['decrypted_key'] = decrypted_key
        except asyncio.TimeoutError:
            self.__logger.error("Timeout getting decryption key, aborting")
            return
        except KeyRetrievalException as e:
            self.__logger.error("process_job Error getting key for job, aborting processing: %s" % str(e))
            return

        try:
            await self._run_job(job)
        except:
            self.__logger.exception("Unhandled exception in process_job - will retry")

    async def _get_key(self, job: dict) -> bytes:
        producer = await self._context.get_producer()
        response = await producer.command({'job_id': job['job_id'], 'queue': self.get_job_type()},
                                          'GrosFichiers', 'jobGetKey', Constantes.SECURITE_PRIVE,
                                          domain_check=["GrosFichiers", "MaitreDesCles"])
        parsed = response.parsed

        if parsed.get('ok') is not True:
            raise KeyRetrievalException('Error getting key: %s' % parsed.get('err'))

        decrypted_key = parsed['cles'][0]['cle_secrete_base64']
        decrypted_key_bytes: bytes = multibase.decode('m'+decrypted_key)

        return decrypted_key_bytes

    async def _run_job(self, job: Union[dict, VersionJob]):
        dir_staging = self._context.dir_media_staging

        version = job.get('version') or job
        # Downloader/dechiffrer
        fuuid = version['fuuid']
        mimetype = version['mimetype']

        # Les videos (ffmpeg) utilisent un fichier avec nom
        if est_video(mimetype):
            class_tempfile = tempfile.NamedTemporaryFile
        else:
            class_tempfile = tempfile.TemporaryFile

        self.__logger.debug("Download %s", fuuid)
        tmp_file = class_tempfile(dir=dir_staging)
        try:
            try:
                await self._downloader_dechiffrer_fichier(job['decrypted_key'], version, tmp_file)
                tmp_file.seek(0)  # Rewind pour traitement
                self.__logger.debug("File has been decrypted (fp tmp)")
            except:
                self.__logger.exception("Unhandled exception in download - will retry later")
                return

            try:
                # Traitement
                await self._traiter_fichier(job, tmp_file)
            except Exception as e:
                self.__logger.exception("Erreur traitement - annuler pour %s : %s", job, e)
                await self.annuler_job(job, True)
        finally:
            if tmp_file.closed is False:
                tmp_file.close()

    async def _traiter_fichier(self, job: dict, tmp_file) -> dict:
        raise NotImplementedError('must override')

    async def annuler_job(self, job, emettre_evenement=False):
        raise NotImplementedError('must override')

    async def _downloader_dechiffrer_fichier(self, decrypted_key: bytes, job: Union[dict, TypedDict], tmp_file):
        fuuid = job['fuuid']
        filehost_url = self._context.filehost_url
        url_fichier = urljoin(filehost_url, f'filehost/files/{fuuid}')

        for i in range(0, 3):
            tmp_file.seek(0)  # Put file back at beginning
            decipher = get_decipher_cle_secrete(decrypted_key, job)

            if self.__session is None:
                timeout = aiohttp.ClientTimeout(connect=5, total=600)
                connector = self._context.get_tcp_connector()
                session = aiohttp.ClientSession(timeout=timeout, connector=connector)
                session.verify = self._context.tls_method != 'nocheck'
                try:
                    await filehost_authenticate(self._context, session)
                    self.__session = session
                except aiohttp.ClientResponseError:
                    self.__logger.exception("Error authenticating")
                    await self._context.wait(2)
                    continue  # Retry

            try:
                async with self.__session.get(url_fichier) as resp:
                    resp.raise_for_status()

                    async for chunk in resp.content.iter_chunked(64*1024):
                        tmp_file.write(decipher.update(chunk))

                tmp_file.write(decipher.finalize())

                return  # Success
            except aiohttp.ClientResponseError as cre:
                if cre.status in [400, 401, 403]:
                    session = self.__session
                    self.__session = None
                    if session is not None:
                        await session.close()
                    continue  # Retry with new session
                elif 500 <= cre.status < 600:
                    # Server error, wait and retry
                    await self._context.wait(2)
                    continue
                else:
                    raise cre

        raise Exception('Too many retries')

    async def new_file(self, event: MessageWrapper):
        pass

class IntakeJobPuller(IntakeHandler):
    """
    Pulls jobs from GrosFichiers. Listens to new file events.
    """
    def __init__(self, context: MediaContext, qsize=1):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        super().__init__(context)
        self._file_event = asyncio.Event()
        self._processing_queue: asyncio.Queue[Optional[VersionJob]] = asyncio.Queue(maxsize=qsize)

    async def run(self):
        async with TaskGroup() as group:
            group.create_task(self.__fetch_jobs_thread())
            group.create_task(self.__process_jobs_thread())
            group.create_task(self.__stop_thread())

    async def __stop_thread(self):
        await self._context.wait()
        # Free all threads
        self._file_event.set()
        await self._processing_queue.put(None)

    async def new_file(self, event: MessageWrapper):
        self._file_event.set()

    async def __fetch_jobs_thread(self):
        while self._context.stopping is False:
            self._file_event.clear()
            try:
                self.__logger.debug("Fetching new jobs")
                await self.__fetch_jobs()
            except:
                self.__logger.exception("Error fetching jobs")
            try:
                await asyncio.wait_for(self._file_event.wait(), 300)
            except asyncio.TimeoutError:
                pass

    async def __fetch_jobs(self):
        batch_size = self._processing_queue.maxsize - self._processing_queue.qsize()
        try:
            filehost_id = self._context.filehost.filehost_id
        except (AttributeError, KeyError):
            # Filehost not loaded, wait a while
            await self._context.wait(5)
            filehost_id = self._context.filehost.filehost_id

        command = {'batch_size': batch_size, 'filehost_id': filehost_id}
        producer = await self._context.get_producer()
        response = await producer.command(command, Constantes.DOMAINE_GROS_FICHIERS, "leaseForImage", Constantes.SECURITE_PRIVE)
        await self.__process_response(response.parsed)

    async def __process_response(self, response: dict):
        if response.get('ok') is not True:
            self.__logger.error(f"Error getting jobs: {response.get('err')}")
            return
        elif response.get('code') == 1:
            self.__logger.debug("No more jobs to process")
            return

        keys: dict[str, SecretKeyDict] = dict()
        for key in response['secret_keys']:
            keys[key['cle_id']] = key

        leases = response['leases']
        for lease in leases:
            # Inject keys in leases
            lease['keys'] = keys
            # Add job to queue
            await self._processing_queue.put(lease)

    async def __process_jobs_thread(self):
        while self._context.stopping is False:
            if self._processing_queue.qsize() == 0:
                self._file_event.set()  # Q empty, try to fetch new jobs

            job = await self._processing_queue.get()

            if self._context.stopping or job is None:
                return  # Stopping

            try:
                await self._process_version_job(job)
            except:
                self.__logger.exception("Error processing job")

    async def _process_version_job(self, job: VersionJob):
        raise NotImplementedError('must implement')


class IntakeJobImage(IntakeJobPuller):

    def __init__(self, context: MediaContext):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        super().__init__(context, qsize=5)

    def get_job_type(self) -> str:
        return 'image'

    async def _process_version_job(self, job: VersionJob):
        # Find key to decrypt file
        decrypted_key, key_info = decrypt_job_key(job)
        key_info.update(job)  # Inject nonce, format
        job['decrypted_key'] = decrypted_key  # Inject key for update of results

        version = job['version']

        # Download/decrypt
        fuuid = version['fuuid']

        self.__logger.debug("_process_version_job Download %s", fuuid)
        dir_staging = self._context.dir_media_staging
        with tempfile.TemporaryFile(dir=dir_staging) as tmp_file:
            try:
                await self._downloader_dechiffrer_fichier(decrypted_key, version, tmp_file)
                tmp_file.seek(0)  # Rewind for processing
                self.__logger.debug("File has been decrypted (fp tmp)")
            except:
                self.__logger.exception("Unhandled exception in download - will retry later")
                return

            try:
                # Create new image dict to pass in the decrypted key (old way)
                image_info = version.copy()
                image_info['decrypted_key'] = job['decrypted_key']
                await traiter_image(image_info, tmp_file, self._context)
            except Exception as e:
                self.__logger.exception("Erreur traitement - annuler pour %s : %s", job, e)
                await self.annuler_job(job, True)

    async def annuler_job(self, job, emettre_evenement=False):
        if not emettre_evenement:
            return

        fuuid = job.get('fuuid') or job['version']['fuuid']

        command = {'fuuid': fuuid}

        producer = await self._context.get_producer()
        await producer.command(
            command, 'GrosFichiers', 'supprimerJobImageV2', exchange=Constantes.SECURITE_PRIVE,
            nowait=True
        )


class IntakeJobVideo(IntakeHandler):

    def __init__(self, context: MediaContext):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        super().__init__(context)
        self.__job_handler: Optional[VideoConversionJob] = None

    def get_job_type(self) -> str:
        return 'video'

    async def _traiter_fichier(self, job: dict, tmp_file):
        self.__logger.debug("Traiter video %s" % job['job_id'])

        mimetype: str = job['mimetype']
        if est_video(mimetype) is False:
            raise ValueError("Only video mimetypes are supported")

        if self.__job_handler is not None:
            raise Exception('1 seule thread permise a la fois')

        try:
            self.__job_handler = VideoConversionJob(self._context, job, tmp_file)
            self.__logger.debug("Debut traiter video de job_handler")
            await self.__job_handler.traiter_video()
        finally:
            job_handler = self.__job_handler
            self.__job_handler = None
            await job_handler.annuler()  # Aucun effet si la job s'est terminee correctement
        self.__logger.debug("Fin traiter video de job_handler")

    async def annuler_job(self, job_id: str, emettre_commande=False):
        if self.__job_handler is not None:
            job_courante = self.__job_handler.job
            # Verifier si on doit annuler la job en cours
            try:
                if job_id == job_courante['job_id']:
                    self.__logger.info("Annuler job courante job_id:%s, fuuid:%s", job_id, job_courante['fuuid'])
                    await self.__job_handler.annuler()
                else:
                    self.__logger.debug("annuler_job courante : mismatch, on ne fait rien")
            except KeyError:
                self.__logger.debug("annuler_job courante : mismatch keys, on ne fait rien")
        else:
            self.__logger.debug("annuler_job courante : aucune job courante - emettre message d'annulation")


class KeyRetrievalException(Exception):
    pass


def decrypt_job_key(job: VersionJob) -> (bytes, dict):
    version = job['version']
    key_id = version.get('cle_id') or version['fuuid']
    key = job['keys'][key_id]
    decrypted_key = key['cle_secrete_base64']
    decrypted_key_bytes: bytes = multibase.decode('m'+decrypted_key)

    return decrypted_key_bytes, key
