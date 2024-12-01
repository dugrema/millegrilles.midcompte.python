import asyncio
from urllib.parse import urljoin

import aiohttp
import logging
import tempfile
import multibase

from typing import Optional
from ssl import SSLContext

from asyncio import Event

from millegrilles_messages.messages import Constantes
from millegrilles_media.Context import MediaContext
from millegrilles_media.TransfertFichiers import filehost_authenticate
from millegrilles_messages.chiffrage.DechiffrageUtils import get_decipher_cle_secrete
from millegrilles_messages.Mimetypes import est_video
from millegrilles_media.ImagesHandler import traiter_image
from millegrilles_media.VideosHandler import VideoConversionJob


class IntakeHandler:

    def __init__(self, context: MediaContext):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self._context = context
        self.__event_fichiers: Event = None
        self.__ssl_context: Optional[SSLContext] = None

    async def configurer(self):
        self.__event_fichiers = Event()

        config = self._context.configuration
        cert_path = config.cert_pem_path
        self.__ssl_context = SSLContext()
        self.__ssl_context.load_cert_chain(cert_path, config.key_pem_path)

    def get_job_type(self) -> str:
        raise NotImplementedError('must implement')

    async def process_job(self, job: dict):
        try:
            decrypted_key: bytes = await self.__get_key(job)
            job['decrypted_key'] = decrypted_key
        except asyncio.TimeoutError:
            self.__logger.error("Timeout getting decryption key, aborting")
            return
        except KeyRetrievalException as e:
            self.__logger.error("process_job Error getting key for job, aborting processing: %s" % str(e))
            return

        try:
            await self.__run_job(job)
        except:
            self.__logger.exception("Unhandled exception in process_job - will retry")

    async def __get_key(self, job: dict) -> bytes:
        producer = await self._context.get_producer()
        response = await producer.command({'job_id': job['job_id'], 'queue': self.get_job_type()},
                                          'GrosFichiers', 'jobGetKey', Constantes.SECURITE_PROTEGE,
                                          domain_check=["GrosFichiers", "MaitreDesCles"])
        parsed = response.parsed

        if parsed.get('ok') is not True:
            raise KeyRetrievalException('Error getting key: %s' % parsed.get('err'))

        decrypted_key = parsed['cles'][0]['cle_secrete_base64']
        decrypted_key_bytes: bytes = multibase.decode('m'+decrypted_key)

        return decrypted_key_bytes

    async def __run_job(self, job: dict):
        dir_staging = self._context.dir_media_staging

        # Downloader/dechiffrer
        fuuid = job['fuuid']
        mimetype = job['mimetype']

        # Les videos (ffmpeg) utilisent un fichier avec nom
        if est_video(mimetype):
            class_tempfile = tempfile.NamedTemporaryFile
        else:
            class_tempfile = tempfile.TemporaryFile

        self.__logger.debug("Download %s", fuuid)
        tmp_file = class_tempfile(dir=dir_staging)
        try:
            try:
                await self._downloader_dechiffrer_fichier(job['decrypted_key'], job, tmp_file)
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

    async def _downloader_dechiffrer_fichier(self, decrypted_key: bytes, job: dict, tmp_file):
        fuuid = job['fuuid']
        decipher = get_decipher_cle_secrete(decrypted_key, job)

        timeout = aiohttp.ClientTimeout(connect=5, total=600)
        connector = self._context.get_tcp_connector()
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            session.verify = self._context.tls_method != 'nocheck'

            await filehost_authenticate(self._context, session)

            filehost_url = self._context.filehost_url
            url_fichier = urljoin(filehost_url, f'filehost/files/{fuuid}')
            async with session.get(url_fichier) as resp:
                resp.raise_for_status()

                async for chunk in resp.content.iter_chunked(64*1024):
                    tmp_file.write(decipher.update(chunk))

        tmp_file.write(decipher.finalize())


class IntakeJobImage(IntakeHandler):

    def __init__(self, context: MediaContext):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        super().__init__(context)

    def get_job_type(self) -> str:
        return 'image'

    async def _traiter_fichier(self, job, tmp_file):
        self.__logger.debug("Traiter image %s" % job)
        mimetype: str = job['mimetype']

        if est_video(mimetype):
            raise ValueError("Mimetype video/* is not supported")
            # await traiter_poster_video(job, tmp_file, self._context)
        else:
            await traiter_image(job, tmp_file, self._context)

    async def annuler_job(self, job, emettre_evenement=False):
        if not emettre_evenement:
            return

        reponse = {
            'job_id': job['job_id'],
            'tuuid': job['tuuid'],
            'fuuid': job['fuuid'],
        }

        producer = await self._context.get_producer()
        await producer.command(
            reponse, 'GrosFichiers', 'supprimerJobImageV2', exchange='3.protege',
            nowait=True
        )


class IntakeJobVideo(IntakeHandler):

    def __init__(self, context: MediaContext):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        super().__init__(context)
        self.__job_handler = None

    def get_job_type(self) -> str:
        return 'video'

    async def _traiter_fichier(self, job, tmp_file):
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

    async def annuler_job(self, job, emettre_commande=False):
        if self.__job_handler is not None:
            job_courante = self.__job_handler.job
            # Verifier si on doit annuler la job en cours
            try:
                if job['fuuid'] == job_courante['fuuid'] and \
                      job['user_id'] == job_courante['user_id'] and \
                      job['cle_conversion'] == job_courante['cle_conversion']:
                    self.__logger.info("Annuler job courante %s %s" % (job['fuuid'], job['cle_conversion']))
                    await self.__job_handler.annuler()
                else:
                    self.__logger.debug("annuler_job courante : mismatch, on ne fait rien")
            except KeyError:
                self.__logger.debug("annuler_job courante : mismatch keys, on ne fait rien")
        else:
            self.__logger.debug("annuler_job courante : aucune job courante - emettre message d'annulation")

        if emettre_commande:
            commande = {
                'ok': False,
                'job_id': job['job_id'],
                'fuuid': job['fuuid'],
                'tuuid': job['tuuid'],
                'user_id': job['user_id'],
            }

            producer = await self._context.get_producer()
            await producer.command(
                commande, 'GrosFichiers', 'supprimerJobVideoV2', exchange='3.protege',
                nowait=True
            )


class KeyRetrievalException(Exception):
    pass
