# Intake de fichiers a indexer
import aiohttp
import logging
import json
import tempfile
import multibase

from typing import Optional
from ssl import SSLContext

from asyncio import Event, TimeoutError, wait, FIRST_COMPLETED, gather

from millegrilles_messages.messages.Hachage import convertir_hachage_mb_hex
from millegrilles_messages.messages.MessagesModule import MessageWrapper
from millegrilles_messages.chiffrage.DechiffrageUtils import dechiffrer_document_secrete, get_decipher_cle_secrete
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

    async def reset_index_fichiers(self):
        self.__logger.info('IntakeHandler trigger fichiers recu')

        # Bloquer temporairement traitement de l'indexation
        self.__event_fichiers.clear()

        # Supprimer tous les documents indexes
        await self.__solr_dao.reset_index(self.__solr_dao.nom_collection_fichiers)

        # Redemarrer l'indexaction
        self.__event_fichiers.set()

    async def supprimer_tuuids(self, message: MessageWrapper):
        contenu = message.parsed
        tuuids = contenu['tuuids']
        self.__logger.debug("Supprimer tuuids de l'index: %s" % tuuids)
        await self.__solr_dao.supprimer_tuuids(self.__solr_dao.nom_collection_fichiers, tuuids)
        return {'ok': True}

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
                    user_id = job['user_id']
                    tuuid = job['tuuid']

                    # Downloader/dechiffrer
                    try:
                        fuuid = job['fuuid']
                        mimetype = job['mimetype']
                        if fuuid is None or mimetype is None:
                            raise TypeError('fuuid ou mimetype None')
                    except (KeyError, TypeError):
                        # Aucun fichier (e.g. un repertoire
                        mimetype = None
                        fuuid = None
                        tmp_file = None
                    else:
                        if mimetype_supporte_fulltext(mimetype):
                            self.__logger.debug("traiter_fichiers Downloader %s" % fuuid)
                            tmp_file = tempfile.TemporaryFile()
                            try:
                                await self.downloader_dechiffrer_fichier(job, tmp_file)
                                tmp_file.seek(0)
                            except aiohttp.ClientResponseError as e:
                                if e.status == 404:
                                    self.__logger.warning("Fichier absent (404) : %s - contenu non indexe" % fuuid)
                                    tmp_file.close()
                                    tmp_file = None
                                else:
                                    raise e
                            except FichierVide:
                                self.__logger.warning("Fichier vide : %s - contenu non indexe" % fuuid)
                                tmp_file.close()
                                tmp_file = None

                            self.__logger.debug("Fichier a indexer est dechiffre (fp tmp)")
                        else:
                            tmp_file = None

                    info_fichier = await self.dechiffrer_metadata(job)
                    info_fichier['mimetype'] = mimetype
                    # info_fichier['tuuid'] = tuuid
                    info_fichier['fuuid'] = fuuid

                    try:
                        hachage_original = info_fichier['hachage_original']
                        # Decoder la valeur multihash et re-encoder en hex
                        hachage_original_hex = convertir_hachage_mb_hex(hachage_original)
                        info_fichier['hachage_original'] = hachage_original_hex
                    except KeyError:
                        pass  # OK

                    try:
                        cuuids: Optional[list] = job['path_cuuids']
                        if cuuids is not None:
                            # Path cuuids commence par le parent immediat (idx:0 est le parent)
                            # Inverser l'ordre pour l'indexation
                            cuuids.reverse()
                            # info_fichier['cuuids'] = '/'.join(cuuids)
                            info_fichier['cuuids'] = cuuids
                    except KeyError:
                        pass  # Ok

                    self.__logger.debug("Indexer fichier %s" % json.dumps(info_fichier, indent=2))

                    # Indexer
                    await self.__solr_dao.indexer(
                        self.__solr_dao.nom_collection_fichiers, user_id, tuuid, info_fichier, tmp_file)

                    # Confirmer succes de l'indexation
                    producer = self._etat_relaisolr.producer
                    await producer.executer_commande(
                        {'fuuid': fuuid, 'user_id': user_id, 'tuuid': tuuid},
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
            if job_indexation.parsed['ok'] is True:
                self.__logger.debug("Executer job indexation : %s" % job_indexation)
                return job_indexation.parsed
            else:
                self.__logger.debug("Aucune job d'indexation disponible")
        except Exception as e:
            self.__logger.error("Erreur recuperation job indexation : %s" % e)

        return None

    async def dechiffrer_metadata(self, job):
        # cle = job['cle']['cle']
        information_dechiffrage = job['cle']
        cle: bytes = multibase.decode('m' + information_dechiffrage['cle_secrete_base64'])
        metadata = job['metadata']

        doc_dechiffre = dechiffrer_document_secrete(cle, metadata)
        return doc_dechiffre

    async def downloader_dechiffrer_fichier(self, job, tmp_file):
        # cle = job['cle']['cle']
        information_dechiffrage = job['cle']
        cle: bytes = multibase.decode('m' + information_dechiffrage['cle_secrete_base64'])

        if job.get('cle_id'):
            # Ajouter encodage multibase pour ancien format
            job['nonce'] = 'm' + job['nonce']

        fuuid = job['fuuid']
        # clecert = self._etat_relaisolr.clecertificat
        # decipher = get_decipher(clecert, cle, job['cle'])

        if job.get('cle_id'):
            information_dechiffrage = job.copy()
            # Ajouter encodage multibase pour ancien format
            # information_dechiffrage['nonce'] = 'm' + information_dechiffrage['nonce']
        else:
            information_dechiffrage = job['cle']  # Supporter ancienne approche

        decipher = get_decipher_cle_secrete(cle, information_dechiffrage)

        url_consignation = self._etat_relaisolr.url_consignation
        url_fichier = f'{url_consignation}/fichiers_transfert/{fuuid}'

        taille_fichier = 0
        timeout = aiohttp.ClientTimeout(connect=5, total=240)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url_fichier, ssl=self.__ssl_context) as resp:
                resp.raise_for_status()

                async for chunk in resp.content.iter_chunked(64*1024):
                    tmp_file.write(decipher.update(chunk))
                    taille_fichier = taille_fichier + len(chunk)

        try:
            dernier_chunk = decipher.finalize()
            tmp_file.write(dernier_chunk)
            taille_fichier = taille_fichier + len(dernier_chunk)
            if taille_fichier == 0:
                raise FichierVide('fuuid %s est vide' % fuuid)
        except ValueError as e:
            raise FichierVide('fuuid %s est vide (err: %s)' % (fuuid, e))

        # DEBUG
        # tmp_file.seek(0)
        # with open('/tmp/output.pdf', 'wb') as fichier:
        #     fichier.write(tmp_file.read())


MIMETYPES_FULLTEXT = [
    'application/pdf',
    'application/vnd.ms-powerpoint',
    'application/msword',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
]
BASE_FULLTEXT = ['text']


def mimetype_supporte_fulltext(mimetype) -> bool:

    if mimetype in MIMETYPES_FULLTEXT:
        return True
    else:
        prefix = mimetype.split('/')[0]
        if prefix in BASE_FULLTEXT:
            return True

    return False


class FichierVide(Exception):
    pass
