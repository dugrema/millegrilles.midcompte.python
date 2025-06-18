# Intake de fichiers a indexer
from asyncio import TaskGroup
from json import JSONDecodeError
from urllib.parse import urljoin

import aiohttp
import asyncio
import logging
import json
import tempfile
import multibase

from typing import Optional

from millegrilles_messages.messages import Constantes
from millegrilles_messages.messages.Hachage import convertir_hachage_mb_hex
from millegrilles_messages.chiffrage.DechiffrageUtils import dechiffrer_document_secrete, get_decipher_cle_secrete
from millegrilles_solr.Context import SolrContext
from millegrilles_solr.solrdao import SolrDao


class IntakeHandler:

    def __init__(self, context: SolrContext, solr_dao: SolrDao):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__context = context
        self.__solr_dao = solr_dao
        # self.__event_fichiers = asyncio.Event()
        self.__event_fetch_jobs = asyncio.Event()
        self.__processing_queue: asyncio.Queue[Optional[dict]] = asyncio.Queue(maxsize=100)
        self.__session: Optional[aiohttp.ClientSession] = None

    async def trigger_fetch_jobs(self):
        self.__event_fetch_jobs.set()
        return {'ok': True}

    async def run(self):
        async with TaskGroup() as group:
            group.create_task(self.__stop_thread())
            group.create_task(self.__fetch_jobs_timer())
            group.create_task(self.__fetch_jobs_thread())
            group.create_task(self.__process_queue())

    async def __stop_thread(self):
        await self.__context.wait()
        # Free resources
        await self.__processing_queue.put(None)
        self.__event_fetch_jobs.set()

    async def __fetch_jobs_timer(self):
        """
        Triggers a fetch operation regularly
        :return:
        """
        while self.__context.stopping is False:
            self.__event_fetch_jobs.set()
            await self.__context.wait(300)

    async def __fetch_jobs_thread(self):
        while self.__context.stopping is False:
            self.__event_fetch_jobs.clear()
            try:
                await self.__fetch_jobs()
            except:
                self.__logger.exception("Unhandled error fetching jobs")
            await self.__event_fetch_jobs.wait()

    async def __fetch_jobs(self):
        # Fetch enough items to fill the queue
        batch_size = self.__processing_queue.maxsize - self.__processing_queue.qsize()
        if batch_size == 0:
            return  # Queue full

        try:
            filehost_id = self.__context.filehost.filehost_id
        except AttributeError:
            # The filehost is not loaded, wait and try again
            await self.__context.wait(5)
            filehost_id = self.__context.filehost.filehost_id

        command = {'batch_size': batch_size, 'filehost_id': filehost_id}
        producer = await self.__context.get_producer()
        response = await producer.command(command, Constantes.DOMAINE_GROS_FICHIERS, "leaseForIndex", Constantes.SECURITE_PROTEGE, timeout=60)
        if response.parsed.get('ok') is True:
            response_code = response.parsed.get('code')
            self.__logger.debug(f"Response code: {response_code}, response message: {response.parsed.get('message')}")
            if response_code == 1:
                pass  # No more files available to index
            else:
                await self.__process_response(response.parsed)
        else:
            self.__logger.error(f"Error fetching files: {response.parsed.get('err')}")

    async def __process_response(self, message: dict):
        leases = message['leases']
        secret_keys = message['secret_keys']

        # Index keys by cle_id
        keys = dict()
        for key in secret_keys:
            keys[key['cle_id']] = key

        # Inject key dict in all jobs
        for job in leases:
            job['keys'] = keys
            await self.__processing_queue.put(job)

    async def __process_queue(self):
        while self.__context.stopping is False:
            if self.__processing_queue.qsize() == 0:
                # The queue is emptied, fetch more jobs when available
                self.__event_fetch_jobs.set()

            job = await self.__processing_queue.get()
            if job is None or self.__context.stopping:
                return  # Stopping

            try:
                try:
                    await self.__process_job(job)
                except* Exception:
                    self.__logger.exception("Error* processing job")
                    await self.annuler_job(job, True)
            except Exception:
                self.__logger.exception("Error processing job")
                await self.annuler_job(job, True)

    async def __process_job(self, job: dict):
        try:
            decrypted_key, key = await self.__get_key(job)
            job['decrypted_key'] = decrypted_key
            job['key'] = key
        except (TypeError, KeyError) as e:
            self.__logger.error(f"Job fuuid:{job.get('fuuid')}, missing decryption key, SKIPPING. Error: {str(e)}")
            await self.annuler_job(job, emettre_evenement=True, err=e)
            return
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

    async def __get_key(self, job: dict) -> (bytes, dict):
        metadata = job['metadata']
        key_id = metadata.get('cle_id') or metadata.get('ref_hachage_bytes') or job['version']['fuuid']
        key = job['keys'][key_id]
        decrypted_key = key['cle_secrete_base64']
        decrypted_key_bytes: bytes = multibase.decode('m'+decrypted_key)

        return decrypted_key_bytes, key

    async def __run_job(self, job: dict):
        # Downloader/dechiffrer
        tuuid = job['tuuid']
        try:
            user_id = job['user_id']
        except KeyError:
            self.__logger.warning("user_id manquant de la job: %s" % job)
            await self.annuler_job(job, emettre_evenement=True)
            return

        try:
            version = job['key'].copy()
            # Add all job info to the key, copies the format/nonce when using new format
            version.update(job['version'])

            fuuid = version['fuuid']
            mimetype = job.get('mimetype') or version['mimetype']

        except (KeyError, TypeError):
            # Aucun fichier (e.g. un repertoire
            mimetype = None
            fuuid = None
            version = None

        # Traiter le fichier avec indexation du contenu si applicable
        try:
            if mimetype_supporte_fulltext(mimetype) and fuuid:
                with tempfile.TemporaryFile() as tmp_file:

                    try:
                        await self.__downloader_dechiffrer_fichier(job['decrypted_key'], version, tmp_file)
                    except:
                        self.__logger.exception("Unhandled exception in download - will retry later")
                        return
                    tmp_file.seek(0)  # Rewind pour traitement
                    self.__logger.debug("Fichier a indexer est dechiffre (fp tmp)")
                    await self.__traiter_fichier(job, tmp_file)
                return  # Indexation avec contenu terminee
        except FichierVide:
            # Aucun contenu du fichier, traiter comme un fichier dont le contenu n'est pas indexable
            pass
        except Exception as e:
            self.__logger.exception("Erreur traitement - annuler pour %s : %s" % (job, e))
            await self.annuler_job(job, True)
            return

        # Traitement sans indexation du contenu
        try:
            await self.__traiter_fichier(job, None)
        except Exception as e:
            self.__logger.exception("Erreur traitement - annuler pour %s : %s" % (job, e))
            await self.annuler_job(job, True)

    async def __traiter_fichier(self, job: dict, tmp_file: Optional[tempfile.TemporaryFile]):
        user_id = job['user_id']
        tuuid = job['tuuid']

        try:
            version = job['version']
            fuuid = version['fuuid']
            mimetype = job.get('mimetype') or version['mimetype']
        except (KeyError, TypeError):
            # Aucun fichier (e.g. un repertoire
            mimetype = None
            fuuid = None
            tmp_file = None

        try:
            info_fichier = await self.__dechiffrer_metadata(job)
        except (UnicodeDecodeError, JSONDecodeError) as e:
            self.__logger.error(f"intake.__traiter_fichier Error decrypting file tuuid:{tuuid} metadata, SKIPPING. Error: {str(e)}")
            await self.annuler_job(job, emettre_evenement=True, err=e)
            return

        info_fichier['mimetype'] = mimetype
        info_fichier['fuuid'] = fuuid

        try:
            hachage_original = info_fichier['hachage_original']
            # Decoder la valeur multihash et re-encoder en hex
            hachage_original_hex = convertir_hachage_mb_hex(hachage_original)
            info_fichier['hachage_original'] = hachage_original_hex
        except KeyError:
            pass  # OK

        try:
            cuuids: Optional[list] = job['cuuids']
            if cuuids is not None:
                # Path cuuids commence par le parent immediat (idx:0 est le parent)
                # Inverser l'ordre pour l'indexation
                cuuids.reverse()
                # info_fichier['cuuids'] = '/'.join(cuuids)
                info_fichier['cuuids'] = cuuids
        except KeyError:
            pass  # Ok

        self.__logger.debug("Indexer fichier tuuid:%s\n%s" % (tuuid, json.dumps(info_fichier, indent=2)))

        # Indexer
        await self.__solr_dao.indexer(
            self.__solr_dao.nom_collection_fichiers, user_id, tuuid, info_fichier, tmp_file)

        # Confirmer succes de l'indexation
        producer = await self.__context.get_producer()
        await producer.command(
            {'ok': True, 'user_id': user_id, 'tuuid': tuuid},
            'GrosFichiers', 'confirmIndex', exchange='3.protege',
            nowait=True
        )

    async def annuler_job(self, job, emettre_evenement=False, err: Optional[Exception] = None):
        if not emettre_evenement:
            return

        reponse = {
            'ok': False,
            'tuuid': job['tuuid'],
            'user_id': job['user_id'],
            'supprimer': True,
        }

        if err:
            reponse['err'] = str(err)

        producer = await self.__context.get_producer()
        await producer.command(
            reponse, 'GrosFichiers', 'confirmIndex', exchange='3.protege',
            nowait=True
        )

    async def __dechiffrer_metadata(self, job):
        cle: bytes = job['decrypted_key']
        metadata = job['metadata']
        doc_dechiffre = dechiffrer_document_secrete(cle, metadata)
        return doc_dechiffre

    async def __downloader_dechiffrer_fichier(self, decrypted_key: bytes, job: dict, tmp_file: tempfile.TemporaryFile) -> int:
        fuuid = job['fuuid']

        file_size = 0

        for i in range(0, 3):
            decipher = get_decipher_cle_secrete(decrypted_key, job)
            tmp_file.seek(0)  # Ensure we are at the beginning in case of client issue later on

            if self.__session is None:
                timeout = aiohttp.ClientTimeout(connect=5, total=600)
                connector = self.__context.get_tcp_connector()
                session = aiohttp.ClientSession(timeout=timeout, connector=connector)
                session.verify = self.__context.tls_method != 'nocheck'
                try:
                    await filehost_authenticate(self.__context, session)
                    self.__session = session
                except aiohttp.ClientResponseError:
                    self.__logger.exception("Error authenticating")
                    await self.__context.wait(2)
                    continue  # Retry

            filehost_url = self.__context.filehost_url
            url_fichier = urljoin(filehost_url, f'filehost/files/{fuuid}')
            try:
                async with self.__session.get(url_fichier) as resp:
                    resp.raise_for_status()

                    async for chunk in resp.content.iter_chunked(64*1024):
                        await asyncio.to_thread(tmp_file.write, decipher.update(chunk))
                        file_size += len(chunk)

                    # Download successful
                    chunk = decipher.finalize()
                    await asyncio.to_thread(tmp_file.write, chunk)
                    file_size += len(chunk)

                    return file_size
            except aiohttp.ClientResponseError as cre:
                if cre.status in [400, 401, 403]:
                    self.__logger.debug("Not authenticated")

                    # Close session
                    session = self.__session
                    self.__session = None
                    if session:
                        await session.close()

                    continue  # Retry with a new session
                elif 500 <= cre.status < 600:
                    self.__logger.info(f"Filehost server error: {cre.status}")
                    await self.__context.wait(3)  # Wait in case the server is restarting
                    continue  # Retry
                else:
                    raise cre

        raise Exception("Attached file download - Too many retries")

MIMETYPES_FULLTEXT = [
    'application/pdf',
    'application/vnd.ms-powerpoint',
    'application/msword',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
]
BASE_FULLTEXT = ['text']


def mimetype_supporte_fulltext(mimetype: Optional[str]) -> bool:

    if mimetype is None:
        return False

    if mimetype in MIMETYPES_FULLTEXT:
        return True
    else:
        prefix = mimetype.split('/')[0]
        if prefix in BASE_FULLTEXT:
            return True

    return False


async def filehost_authenticate(context: SolrContext, session: aiohttp.ClientSession):
    filehost_url = context.filehost_url
    url_authenticate = urljoin(filehost_url, '/filehost/authenticate')
    authentication_message, message_id = context.formatteur.signer_message(
        Constantes.KIND_COMMANDE, dict(), domaine='filehost', action='authenticate')
    authentication_message['millegrille'] = context.formatteur.enveloppe_ca.certificat_pem
    async with session.post(url_authenticate, json=authentication_message) as resp:
        resp.raise_for_status()


class FichierVide(Exception):
    pass


class KeyRetrievalException(Exception):
    pass

