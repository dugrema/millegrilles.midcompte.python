# Intake de fichiers a indexer
import aiohttp
import logging
import tempfile

from typing import Optional
from ssl import SSLContext

from asyncio import Event, TimeoutError, wait, FIRST_COMPLETED, gather

from millegrilles_messages.chiffrage.DechiffrageUtils import get_decipher
from millegrilles_media.EtatMedia import EtatMedia
from millegrilles_media.ImagesHandler import traiter_image, traiter_poster_video
from millegrilles_media.VideosHandler import traiter_video


class IntakeHandler:

    def __init__(self, stop_event: Event, etat_media: EtatMedia):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self._etat_media = etat_media
        self.__event_fichiers: Event = None
        self.__stop_event = stop_event
        self.__ssl_context: Optional[SSLContext] = None

    async def configurer(self):
        self.__event_fichiers = Event()

        config = self._etat_media.configuration
        cert_path = config.cert_pem_path
        self.__ssl_context = SSLContext()
        self.__ssl_context.load_cert_chain(cert_path, config.key_pem_path)

    async def trigger_traitement(self):
        self.__logger.info('IntakeHandler trigger fichiers recu')
        self.__event_fichiers.set()

    async def run(self):
        self.__logger.info('IntakeHandler running')
        await gather(self.traiter_fichiers())

    async def traiter_fichiers(self):
        while not self.__stop_event.is_set():
            try:
                if self.__event_fichiers.is_set() is False:
                    await wait(
                        [self.__stop_event.wait(), self.__event_fichiers.wait()],
                        timeout=20, return_when=FIRST_COMPLETED
                    )
                    self.__event_fichiers.set()
            except TimeoutError:
                self.__logger.debug("Verifier si fichier disponible pour indexation")
                self.__event_fichiers.set()
            else:
                if self.__stop_event.is_set():
                    self.__logger.info('Arret loop traiter_fichiers')
                    break

            try:
                # Requete prochain fichier
                job = await self.get_prochain_fichier()

                if job is not None:
                    # Downloader/dechiffrer
                    fuuid = job['fuuid']

                    self.__logger.debug("Downloader %s" % fuuid)
                    with tempfile.TemporaryFile() as tmp_file:
                        await self.downloader_dechiffrer_fichier(job, tmp_file)
                        tmp_file.seek(0)  # Rewind pour traitement
                        self.__logger.debug("Fichier a indexer est dechiffre (fp tmp)")
                        try:
                            # Traitement
                            await self.traiter_fichier(job, tmp_file)
                        except Exception as e:
                            self.__logger.exception("Erreur traitement - annuler pour %s : %s" % (job, e))
                            await self.annuler_job(job)
                else:
                    self.__event_fichiers.clear()
            except Exception as e:
                self.__logger.exception("traiter_fichiers Erreur traitement : %s" % e)
                # Erreur generique non geree. Creer un delai de traitement pour poursuivre
                self.__event_fichiers.clear()

    async def traiter_fichier(self, job, tmp_file) -> dict:
        raise NotImplementedError('must override')

    async def get_prochain_fichier(self) -> Optional[dict]:
        raise NotImplementedError('must override')

    async def annuler_job(self, job):
        raise NotImplementedError('must override')

    async def downloader_dechiffrer_fichier(self, job, tmp_file):
        cle = job['cle']['cle']
        fuuid = job['fuuid']
        clecert = self._etat_media.clecertificat
        decipher = get_decipher(clecert, cle, job['cle'])

        url_consignation = self._etat_media.url_consignation
        url_fichier = f'{url_consignation}/fichiers_transfert/{fuuid}'

        timeout = aiohttp.ClientTimeout(connect=5, total=600)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url_fichier, ssl=self.__ssl_context) as resp:
                resp.raise_for_status()

                async for chunk in resp.content.iter_chunked(64*1024):
                    tmp_file.write(decipher.update(chunk))

        tmp_file.write(decipher.finalize())


class IntakeJobImage(IntakeHandler):

    def __init__(self, stop_event: Event, etat_media: EtatMedia):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        super().__init__(stop_event, etat_media)

    async def get_prochain_fichier(self) -> Optional[dict]:
        try:
            producer = self._etat_media.producer
            job = await producer.executer_commande(
                dict(), 'GrosFichiers', 'getJobImage', exchange="4.secure")

            if job.parsed['ok'] is True:
                self.__logger.debug("Executer job image : %s" % job)
                return job.parsed
            else:
                self.__logger.debug("Aucune job d'images disponible")

        except Exception as e:
            self.__logger.exception("Erreur recuperation job image : %s" % e)

        return None

    async def traiter_fichier(self, job, tmp_file) -> dict:
        self.__logger.debug("Traiter image %s" % job)
        await traiter_image(tmp_file)

    async def annuler_job(self, job):
        reponse = {
            'fuuid': job['fuuid'],
            'user_id': job['user_id'],
        }

        producer = self._etat_media.producer
        await producer.executer_commande(
            reponse, 'GrosFichiers', 'supprimerJobImage', exchange='4.secure',
            nowait=True
        )


class IntakeJobVideo(IntakeHandler):

    def __init__(self, stop_event: Event, etat_media: EtatMedia):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        super().__init__(stop_event, etat_media)

    async def get_prochain_fichier(self) -> Optional[dict]:
        try:
            producer = self._etat_media.producer
            job = await producer.executer_commande(
                dict(), 'GrosFichiers', 'getJobVideo', exchange="4.secure")

            if job.parsed['ok'] is True:
                self.__logger.debug("Executer job video : %s" % job)
                return job.parsed
            else:
                self.__logger.debug("Aucune job de videos disponible")

        except Exception as e:
            self.__logger.exception("Erreur recuperation job video : %s" % e)

        return None

    async def traiter_fichier(self, job, tmp_file) -> dict:
        self.__logger.debug("Traiter video %s" % job)

        reponse = {
            'mimetype': job['mimetype'],
            'tuuid': job['tuuid'],
            'fuuid': job['fuuid'],
            'user_id': job['user_id'],
        }

        producer = self._etat_media.producer

        raise NotImplementedError('todo -- transcoder video')

    async def annuler_job(self, job):
        reponse = {
            'ok': False,
            'fuuid': job['fuuid'],
            'cle_conversion': job['cle_conversion'],
            'user_id': job['user_id'],
        }

        producer = self._etat_media.producer
        await producer.executer_commande(
            reponse, 'GrosFichiers', 'supprimerJobVideo', exchange='4.secure',
            nowait=True
        )


MIMETYPES_FULLTEXT = [
    'application/pdf',
    'application/vnd.ms-powerpoint',
    'application/msword',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
]
BASE_FULLTEXT = ['text']


def mimetype_supporte_video(mimetype) -> bool:

    if mimetype in MIMETYPES_FULLTEXT:
        return True
    else:
        prefix = mimetype.split('/')[0]
        if prefix in BASE_FULLTEXT:
            return True

    return False
