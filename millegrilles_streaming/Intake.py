import aiohttp
import asyncio
import datetime
import json
import logging
import pathlib
import multibase

from asyncio import TaskGroup
from typing import Optional, Union
from urllib.parse import urljoin

from millegrilles_messages.bus.BusContext import ForceTerminateExecution
from millegrilles_messages.messages import Constantes
from millegrilles_messages.chiffrage.DechiffrageUtils import get_decipher_cle_secrete

from millegrilles_streaming.Context import StreamingContext
from millegrilles_streaming.Structs import InformationFuuid, FilehostInvalidException

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

    # def clear_session(self):
    #     if self.__session is not None:
    #         self.__loop.call_soon_threadsafe(self.__session.close)
    #     self.__session = None

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
            tmp_file.seek(0)
            job.file_position = 0  # Reset position for user feedback

            if self.__session is None:
                timeout = aiohttp.ClientTimeout(connect=5, total=900)
                session = self._context.get_http_session(timeout)
                try:
                    await filehost_authenticate(self._context, session)
                    self.__session = session
                except aiohttp.ClientResponseError:
                    self.__logger.exception("Error authenticating")
                    await self._context.wait(2)
                    continue  # Retry

            decryption_info = {'nonce': job.file_information.nonce, 'format': job.file_information.format, 'header': job.file_information.header}
            decipher = get_decipher_cle_secrete(decrypted_key, decryption_info)

            try:
                async with self.__session.get(url_fichier) as resp:
                    resp.raise_for_status()

                    async for chunk in resp.content.iter_chunked(64*1024):
                        await asyncio.to_thread(tmp_file.write, decipher.update(chunk))
                        job.file_position += len(chunk)  # User feedback
                    await asyncio.to_thread(tmp_file.write, decipher.finalize())

                    return  # Download successful, no retry
            except aiohttp.ClientResponseError as e:
                if e.status in [400, 401, 403]:
                    session = self.__session
                    self.__session = None
                    if session:
                        await session.close()
                    continue  # Will retry
                elif 500 <= e.status < 600:
                    # Server error, wait a while until retry
                    await self._context.wait(2)
                    continue
                else:
                    raise e

        raise Exception('Too many retries')

    async def get_fuuid_header(self, fuuid: str) -> dict:
        """
        Requete HEAD pour verifier que le fichier existe sur la consignation locale.
        :param fuuid:
        :param session:
        :return:
        """
        file_url = urljoin(self._context.filehost_url, f'filehost/files/{fuuid}')

        for i in range(0, 3):
            timeout = aiohttp.ClientTimeout(connect=5, total=900)
            if self.__session is None:
                session = self._context.get_http_session(timeout)
                await filehost_authenticate(self._context, session)
                self.__session = session

            response = await self.__session.head(file_url)
            if response.status in [401, 403]:
                # Cookie expired, try again
                session = self.__session
                self.__session = None
                if session:
                    await session.close()
                continue
            elif 500 <= response.status < 600:
                # Server error, retry
                await self._context.wait(1)
                continue

            response.raise_for_status()
            return {'taille': response.headers.get('Content-Length'), 'status': response.status}

        raise Exception('Too many retries')

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
        file_header = await self.get_fuuid_header(file.fuuid)

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
    if filehost_url is None:
        raise FilehostInvalidException()
    url_authenticate = urljoin(filehost_url, '/filehost/authenticate')
    authentication_message, message_id = context.formatteur.signer_message(
        Constantes.KIND_COMMANDE, dict(), domaine='filehost', action='authenticate')
    authentication_message['millegrille'] = context.formatteur.enveloppe_ca.certificat_pem
    async with session.post(url_authenticate, json=authentication_message) as resp:
        resp.raise_for_status()
