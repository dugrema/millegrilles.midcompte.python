# Intake de fichiers a indexer
import aiohttp
import logging
import json
import tempfile

from typing import Optional
from ssl import SSLContext

from asyncio import Event, TimeoutError, wait, FIRST_COMPLETED, gather

from millegrilles_messages.chiffrage.DechiffrageUtils import dechiffrer_document, get_decipher
from millegrilles_solr.EtatRelaiSolr import EtatRelaiSolr
from millegrilles_solr.solrdao import SolrDao


class IntakeHandler:

    def __init__(self, stop_event: Event, etat_relai_solr: EtatRelaiSolr, solr_dao: SolrDao):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self._etat_relaisolr = etat_relai_solr
        self.__solr_dao = solr_dao
        self.__event_fichiers: Event = None
        self.__stop_event = stop_event
        self.__ssl_context: Optional[SSLContext] = None

    async def configurer(self):
        self.__event_fichiers = Event()

        config = self._etat_relaisolr.configuration
        cert_path = config.cert_pem_path
        self.__ssl_context = SSLContext()
        self.__ssl_context.load_cert_chain(cert_path, config.key_pem_path)

    async def trigger_fichiers(self):
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
                    mimetype = job['mimetype']
                    user_id = job['user_id']
                    if mimetype_supporte_fulltext(mimetype):
                        self.__logger.debug("Downloader %s" % fuuid)
                        tmp_file = tempfile.TemporaryFile()
                        await self.downloader_dechiffrer_fichier(job, tmp_file)
                        tmp_file.seek(0)  # Rewind pour upload
                        self.__logger.debug("Fichier a indexer est dechiffre (fp tmp)")
                    else:
                        tmp_file = None

                    info_fichier = await self.dechiffrer_metadata(job)
                    info_fichier['mimetype'] = job['mimetype']

                    self.__logger.debug("Indexer fichier %s" % json.dumps(info_fichier, indent=2))

                    # Indexer
                    await self.__solr_dao.indexer(
                        self.__solr_dao.nom_collection_fichiers, user_id, fuuid, info_fichier, tmp_file)

                    # Confirmer succes de l'indexation
                    producer = self._etat_relaisolr.producer
                    await producer.executer_commande(
                        {'fuuid': fuuid, 'user_id': user_id},
                        'GrosFichiers', 'confirmerFichierIndexe', exchange='4.secure',
                        nowait=True
                    )
                else:
                    self.__event_fichiers.clear()
            except Exception as e:
                self.__logger.exception("traiter_fichiers Erreur traitement : %s" % e)
                # Erreur generique non geree. Creer un delai de traitement pour poursuivre
                self.__event_fichiers.clear()

    async def get_prochain_fichier(self) -> Optional[dict]:

        try:
            producer = self._etat_relaisolr.producer
            job_indexation = await producer.executer_commande(
                dict(), 'GrosFichiers', 'getJobIndexation', exchange="4.secure")
            if job_indexation.parsed['ok'] == True:
                self.__logger.debug("Executer job indexation : %s" % job_indexation)
                return job_indexation.parsed
            else:
                self.__logger.debug("Aucune job d'indexation disponible")
        except Exception as e:
            self.__logger.error("Erreur recuperation job indexation : %s" % e)

        return None

    async def dechiffrer_metadata(self, job):
        cle = job['cle']['cle']
        metadata = job['metadata']
        clecert = self._etat_relaisolr.clecertificat
        doc_dechiffre = dechiffrer_document(clecert, cle, metadata)
        return doc_dechiffre

    async def downloader_dechiffrer_fichier(self, job, tmp_file):
        cle = job['cle']['cle']
        fuuid = job['fuuid']
        clecert = self._etat_relaisolr.clecertificat
        decipher = get_decipher(clecert, cle, job['cle'])

        url_consignation = self._etat_relaisolr.url_consignation
        url_fichier = f'{url_consignation}/fichiers_transfert/{fuuid}'

        timeout = aiohttp.ClientTimeout(connect=5, total=240)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url_fichier, ssl=self.__ssl_context) as resp:
                resp.raise_for_status()

                async for chunk in resp.content.iter_chunked(64*1024):
                    tmp_file.write(decipher.update(chunk))

        tmp_file.write(decipher.finalize())

        # DEBUG
        # tmp_file.seek(0)
        # with open('/tmp/output.pdf', 'wb') as fichier:
        #     fichier.write(tmp_file.read())


MIMETYPES_FULLTEXT = ['application/pdf']
BASE_FULLTEXT = ['text']


def mimetype_supporte_fulltext(mimetype) -> bool:

    if mimetype in MIMETYPES_FULLTEXT:
        return True
    else:
        prefix = mimetype.split('/')[0]
        if prefix in BASE_FULLTEXT:
            return True

    return False
