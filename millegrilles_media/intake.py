from urllib.parse import urljoin

import aiohttp
import logging
import tempfile
import multibase

from typing import Optional
from ssl import SSLContext

from asyncio import Event

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

    # async def trigger_traitement(self):
    #     self.__logger.info('IntakeHandler trigger fichiers recu')
    #     self.__event_fichiers.set()

    # async def run(self):
    #     self.__logger.info('IntakeHandler running')
    #     try:
    #         async with TaskGroup() as group:
    #             group.create_task(self.traiter_fichiers())
    #             group.create_task(self.__wait_stop())
    #     except* ForceTerminateExecution:
    #         pass

    # async def __wait_stop(self):
    #     await self._context.wait()
    #     raise ForceTerminateExecution()  # Stopping

    # async def traiter_fichiers(self):
    #
    #     while not self._context.stopping:
    #         try:
    #             if self.__event_fichiers.is_set() is False:
    #                 await asyncio.wait_for(self.__event_fichiers.wait(), timeout=20)
    #                 self.__event_fichiers.set()
    #         except TimeoutError:
    #             self.__logger.debug("Verifier si fichier disponible pour indexation")
    #             self.__event_fichiers.set()
    #         else:
    #             if self._context.stopping:
    #                 self.__logger.info('Arret loop traiter_fichiers')
    #                 break
    #
    #         try:
    #             # Requete prochain fichier
    #             job = await self.get_prochain_fichier()
    #
    #             if job is not None:
    #                 dir_staging = self._context.configuration.dir_staging
    #
    #                 # Downloader/dechiffrer
    #                 fuuid = job['fuuid']
    #                 mimetype = job['mimetype']
    #
    #                 # Les videos (ffmpeg) utilisent un fichier avec nom
    #                 if est_video(mimetype):
    #                 # if mimetype.lower().startswith('video/'):
    #                     class_tempfile = tempfile.NamedTemporaryFile
    #                 else:
    #                     class_tempfile = tempfile.TemporaryFile
    #
    #                 self.__logger.debug("Downloader %s" % fuuid)
    #                 # with class_tempfile() as tmp_file:
    #                 # tmp_file = tempfile.NamedTemporaryFile()
    #                 tmp_file = class_tempfile(dir=dir_staging)
    #                 try:
    #                     await self.downloader_dechiffrer_fichier(job, tmp_file)
    #                     tmp_file.seek(0)  # Rewind pour traitement
    #                     self.__logger.debug("Fichier a indexer est dechiffre (fp tmp)")
    #                     try:
    #                         # Traitement
    #                         await self._traiter_fichier(job, tmp_file)
    #                     except Exception as e:
    #                         self.__logger.exception("Erreur traitement - annuler pour %s : %s" % (job, e))
    #                         await self.annuler_job(job, True)
    #                 finally:
    #                     if tmp_file.closed is False:
    #                         tmp_file.close()
    #
    #             else:
    #                 self.__event_fichiers.clear()
    #         except Exception as e:
    #             self.__logger.exception("traiter_fichiers Erreur traitement : %s" % e)
    #             # Erreur generique non geree. Creer un delai de traitement pour poursuivre
    #             self.__event_fichiers.clear()

    async def process_job(self, job: dict):
        dir_staging = self._context.configuration.dir_staging

        # Downloader/dechiffrer
        fuuid = job['fuuid']
        mimetype = job['mimetype']

        # Les videos (ffmpeg) utilisent un fichier avec nom
        if est_video(mimetype):
            # if mimetype.lower().startswith('video/'):
            class_tempfile = tempfile.NamedTemporaryFile
        else:
            class_tempfile = tempfile.TemporaryFile

        self.__logger.debug("Downloader %s" % fuuid)
        # with class_tempfile() as tmp_file:
        # tmp_file = tempfile.NamedTemporaryFile()
        tmp_file = class_tempfile(dir=dir_staging)
        try:
            await self._downloader_dechiffrer_fichier(job, tmp_file)
            tmp_file.seek(0)  # Rewind pour traitement
            self.__logger.debug("Fichier a indexer est dechiffre (fp tmp)")
            try:
                # Traitement
                await self._traiter_fichier(job, tmp_file)
            except Exception as e:
                self.__logger.exception("Erreur traitement - annuler pour %s : %s" % (job, e))
                await self.annuler_job(job, True)
        finally:
            if tmp_file.closed is False:
                tmp_file.close()

    async def _traiter_fichier(self, job, tmp_file) -> dict:
        raise NotImplementedError('must override')

    # async def get_prochain_fichier(self) -> Optional[dict]:
    #     raise NotImplementedError('must override')

    async def annuler_job(self, job, emettre_evenement=False):
        raise NotImplementedError('must override')

    async def _downloader_dechiffrer_fichier(self, job, tmp_file):
        information_dechiffrage = job['cle'].copy()

        # Ajuster format de cle pour multibase
        cle: bytes = multibase.decode('m' + information_dechiffrage['cle_secrete_base64'])

        fuuid = job['fuuid']
        decipher = get_decipher_cle_secrete(cle, information_dechiffrage)

        timeout = aiohttp.ClientTimeout(connect=5, total=600)
        connector = aiohttp.TCPConnector(ssl=self.__ssl_context)
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

    # async def get_prochain_fichier(self) -> Optional[dict]:
    #     try:
    #         producer = await self._context.get_producer()
    #         filehost_id = self._context.filehost['filehost_id']
    #         requete = {'filehost_id': filehost_id}
    #         job = await producer.executer_commande(
    #             requete, 'GrosFichiers', 'getJobImage', exchange="4.secure")
    #
    #         if job.parsed['ok'] is True:
    #             self.__logger.debug("Executer job image : %s" % job)
    #             return job.parsed
    #         else:
    #             self.__logger.debug("Aucune job d'images disponible")
    #
    #     except Exception as e:
    #         self.__logger.exception("Erreur recuperation job image : %s" % e)
    #
    #     return None

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
            'fuuid': job['fuuid'],
            'user_id': job['user_id'],
        }

        producer = await self._context.get_producer()
        await producer.executer_commande(
            reponse, 'GrosFichiers', 'supprimerJobImage', exchange='4.secure',
            nowait=True
        )


class IntakeJobVideo(IntakeHandler):

    def __init__(self, context: MediaContext):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        super().__init__(context)
        self.__job_handler = None

    # async def get_prochain_fichier(self) -> Optional[dict]:
    #     try:
    #         producer = await self._context.get_producer()
    #         filehost_id = self._context.filehost['filehost_id']
    #         requete = {
    #             'filehost_id': filehost_id,
    #             'fallback': self._context.configuration.fallback_only,
    #         }
    #         job = await producer.executer_commande(
    #             requete, 'GrosFichiers', 'getJobVideo', exchange="4.secure")
    #
    #         if job.parsed['ok'] is True:
    #             self.__logger.debug("Executer job video : %s" % job)
    #             return job.parsed
    #         else:
    #             self.__logger.debug("Aucune job de videos disponible")
    #
    #     except Exception as e:
    #         self.__logger.exception("Erreur recuperation job video : %s" % e)
    #
    #     return None

    async def _traiter_fichier(self, job, tmp_file):
        self.__logger.debug("Traiter video %s" % job)

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
            reponse = {
                'ok': False,
                'fuuid': job['fuuid'],
                'cle_conversion': job['cle_conversion'],
                'user_id': job['user_id'],
            }

            producer = await self._context.get_producer()
            await producer.executer_commande(
                reponse, 'GrosFichiers', 'supprimerJobVideo', exchange='4.secure',
                nowait=True
            )
