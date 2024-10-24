import asyncio
import datetime
import errno
import gzip
import logging
import json
import os
import tempfile
import shutil

import pathlib
import re

from aiohttp import web
from aiohttp.web_exceptions import HTTPClientError
from aiohttp.web_request import Request, StreamResponse
from asyncio import Event
from asyncio.exceptions import TimeoutError
from ssl import SSLContext, VerifyMode
from typing import Optional
from urllib.parse import unquote

from cryptography.x509 import ExtensionNotFound

from millegrilles_fichiers.BackupV2 import lire_header_archive_backup
from millegrilles_messages.messages import Constantes as ConstantesMillegrilles
from millegrilles_messages.messages.Hachage import VerificateurHachage, ErreurHachage, Hacheur

from millegrilles_fichiers import Constantes
from millegrilles_fichiers.Configuration import ConfigurationWeb
from millegrilles_fichiers.EtatFichiers import EtatFichiers
from millegrilles_fichiers.Commandes import CommandHandler
from millegrilles_fichiers.Intake import IntakeFichiers
from millegrilles_fichiers.Consignation import ConsignationHandler
from millegrilles_messages.messages.EnveloppeCertificat import EnveloppeCertificat


CONST_FICHIERS_ACCEPTES_SYNC = frozenset([
    Constantes.FICHIER_RECLAMATIONS_PRIMAIRES,
    Constantes.FICHIER_RECLAMATIONS_INTERMEDIAIRES,
    Constantes.FICHIER_BACKUP_V2
])


class JobVerifierParts:

    def __init__(self, transaction, path_upload: pathlib.Path, hachage: str, cles: Optional[dict] = None):
        self.transaction = transaction
        self.path_upload = path_upload
        self.hachage = hachage
        self.cles = cles
        self.done = asyncio.Event()
        self.valide: Optional[bool] = None
        self.exception: Optional[Exception] = None


class WebServer:

    def __init__(self, etat: EtatFichiers, commandes: CommandHandler, intake: IntakeFichiers, consignation: ConsignationHandler):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__etat = etat
        self.__commandes = commandes
        self.__intake = intake
        self.__consignation = consignation

        self.__app = web.Application()
        self.__stop_event: Optional[Event] = None
        self.__configuration = ConfigurationWeb()
        self.__ssl_context: Optional[SSLContext] = None

        self.__queue_verifier_parts: Optional[asyncio.Queue] = None

        self.__connexions_read_sem = asyncio.BoundedSemaphore(self.__etat.configuration.sem_read_count)
        self.__connexions_write_sem = asyncio.BoundedSemaphore(self.__etat.configuration.sem_write_count)
        self.__connexions_backup_sem = asyncio.BoundedSemaphore(self.__etat.configuration.sem_backup_count)

    def setup(self, configuration: Optional[dict] = None):
        self._charger_configuration(configuration)
        self._preparer_routes()
        self._charger_ssl()

    def _charger_configuration(self, configuration: Optional[dict] = None):
        self.__configuration.parse_config(configuration)

    def get_path_upload_fuuid(self, cn: str, fuuid: str):
        return pathlib.Path(self.__etat.configuration.dir_staging, Constantes.DIR_STAGING_UPLOAD, cn, fuuid)

    def _preparer_routes(self):
        self.__app.add_routes([
            # /fichiers_transfert
            web.get('/fichiers_transfert/job/{fuuid}', self.handle_get_job_fuuid),
            web.get('/fichiers_transfert/{fuuid}', self.handle_get_fuuid),
            web.put('/fichiers_transfert/{fuuid}/{position}', self.handle_put_fuuid),
            web.post('/fichiers_transfert/{fuuid}', self.handle_post_fuuid),
            web.delete('/fichiers_transfert/{fuuid}', self.handle_delete_fuuid),

            # /sync
            web.get('/fichiers_transfert/sync/{fichier}', self.handle_get_fichier_sync),

            # /backup
            #web.post('/fichiers_transfert/backup/verifierFichiers', self.handle_post_backup_verifierfichiers),
            #web.put('/fichiers_transfert/backup/upload/{uuid_backup}/{domaine}/{nomfichier}', self.handle_put_backup),
            #web.get('/fichiers_transfert/backup/{uuid_backup}/{domaine}/{nomfichier}', self.handle_get_backup),

            # /backup_v2
            web.put('/fichiers_transfert/backup_v2/{domaine}/{type_fichier}/{nomfichier}', self.handle_put_backup_v2),
            web.put('/fichiers_transfert/backup_v2/{domaine}/{type_fichier}/{version}/{nomfichier}', self.handle_put_backup_v2),
            web.get('/fichiers_transfert/backup_v2/domaines', self.handle_get_backup_v2_liste_domaines),
            web.get('/fichiers_transfert/backup_v2/{domaine}/archives', self.handle_get_backup_v2_liste_versions),
            web.get('/fichiers_transfert/backup_v2/{domaine}/final', self.handle_get_backup_v2_liste_archives),
            web.get('/fichiers_transfert/backup_v2/{domaine}/archives/{version}', self.handle_get_backup_v2_liste_archives),
            web.get('/fichiers_transfert/backup_v2/{domaine}/final/{nomfichier}', self.handle_get_backup_v2),
            web.get('/fichiers_transfert/backup_v2/{domaine}/archives/{version}/{nomfichier}', self.handle_get_backup_v2),

            # web.get('/fichiers_transfert/backup_v2/{domaine}/{nomfichier}', self.handle_get_backup_v2),
            # web.get('/fichiers_transfert/backup_v2/{domaine}/list', self.handle_get_backup_v2_liste),
        ])

    def _charger_ssl(self):
        self.__ssl_context = SSLContext()
        self.__logger.debug("Charger certificat %s" % self.__configuration.web_cert_pem_path)
        self.__ssl_context.load_cert_chain(self.__configuration.web_cert_pem_path,
                                           self.__configuration.web_key_pem_path)
        self.__ssl_context.load_verify_locations(cafile=self.__configuration.ca_pem_path)
        self.__ssl_context.verify_mode = VerifyMode.CERT_REQUIRED

    async def handle_get_fuuid(self, request: Request):
        async with self.__connexions_read_sem:
            fuuid = request.match_info['fuuid']
            method = request.method
            self.__logger.debug("handle_get_fuuid method %s, fuuid %s" % (method, fuuid))

            # Validation information SSL
            try:
                jwt = request.headers['X-jwt']
                # TODO : valider le JWT et verifier qu'il contient le role 'fichiers'
            except KeyError:
                try:
                    common_name = get_common_name(request)
                except Forbidden:
                    return web.HTTPForbidden()

            # Streamer le fichier
            return await self.stream_reponse(request)

    async def handle_get_job_fuuid(self, request: Request):
        async with self.__connexions_read_sem:
            fuuid = request.match_info['fuuid']
            method = request.method
            self.__logger.debug("handle_get_fuuid method %s, fuuid %s" % (method, fuuid))

            # Validation information SSL
            try:
                common_name = get_common_name(request)
            except Forbidden:
                return web.HTTPForbidden()

            # Verifier si le fichier existe
            try:
                info = await self.__consignation.get_info_fichier(fuuid)
                if info.get('etat_fichier') != Constantes.DATABASE_ETAT_MANQUANT:
                    # Le fichier existe et le traitement est complete
                    return web.json_response({'complet': True})
            except (TypeError, AttributeError, KeyError):
                pass  # OK, le fichier n'existe pas

            # Verifier si le fichier est dans l'intake
            path_intake = self.__intake.get_path_intake_fuuid(fuuid)
            if path_intake.exists():
                return web.json_response({'complet': False, 'en_traitement': True}, status=201)

            # Verifier si la job existe
            path_upload = self.get_path_upload_fuuid(common_name, fuuid)
            if path_upload.exists():
                # La job existe, trouver a quelle position du fichier on est rendu.
                part_max = 0
                position_courante = 0
                for fichier in path_upload.iterdir():
                    if fichier.name.endswith('.part'):
                        part_position = int(fichier.name.split('.')[0])
                        if part_position >= part_max:
                            part_max = part_position
                            stat_fichier = fichier.stat()
                            position_courante = part_position + stat_fichier.st_size

                return web.json_response({'complet': False, 'position': position_courante})

            # Ok, le fichier et la job n'existent pas
            return web.HTTPNotFound()

    async def handle_put_fuuid(self, request: Request) -> StreamResponse:
        async with self.__connexions_write_sem:
            fuuid = request.match_info['fuuid']
            position = request.match_info['position']
            headers = request.headers

            # Afficher info (debug)
            self.__logger.debug("handle_put_fuuid fuuid: %s position: %s" % (fuuid, position))
            for key, value in headers.items():
                self.__logger.debug('handle_put_fuuid key: %s, value: %s' % (key, value))

            try:
                common_name = get_common_name(request)
            except Forbidden:
                return web.HTTPForbidden()

            # S'assurer que le fichier n'existe pas deja
            try:
                info = await self.__consignation.get_info_fichier(fuuid)
                if info['etat_fichier'] != Constantes.DATABASE_ETAT_MANQUANT:
                    self.__logger.info("handle_put_fuuid Fichier %s existe deja, skip" % fuuid)
                    path_upload = self.get_path_upload_fuuid(common_name, fuuid)
                    try:
                        shutil.rmtree(path_upload)
                    except FileNotFoundError:
                        pass  # OK
                    except Exception as e:
                        self.__logger.info("handle_delete_fuuid Erreur suppression upload %s : %s" % (fuuid, e))
                        return web.HTTPServerError()
                    return web.HTTPConflict()
            except (TypeError, AttributeError, KeyError):
                pass  # OK, le fichier n'existe pas

            # content_hash = headers.get('x-content-hash') or headers.get('x-fuuid')
            content_hash = headers.get('x-content-hash')
            try:
                content_length = int(headers['Content-Length'])
            except KeyError:
                content_length = None

            # Creer repertoire pour sauvegader la partie de fichier
            path_upload = self.get_path_upload_fuuid(common_name, fuuid)
            path_upload.mkdir(parents=True, exist_ok=True)

            path_fichier = pathlib.Path(path_upload, '%s.part' % position)
            # S'assurer que le fichier .part n'existe pas deja (on serait en mode resume)
            try:
                stat_fichier = path_fichier.stat()
                if content_length == stat_fichier.st_size:
                    return web.HTTPOk()  # On a deja ce .part de fichier, il a la meme longueur
            except FileNotFoundError:
                pass  # Fichier absent ou taille differente

            path_fichier_work = pathlib.Path(path_upload, '%s.part.work' % position)
            self.__logger.debug("handle_put_fuuid Conserver part %s" % path_fichier)

            if content_hash:
                verificateur = VerificateurHachage(content_hash)
            else:
                verificateur = None
            with open(path_fichier_work, 'wb', buffering=1024*1024) as fichier:
                async for chunk in request.content.iter_chunked(64 * 1024):
                    if verificateur:
                        verificateur.update(chunk)
                    fichier.write(chunk)
                os.fsync(fichier)

            # Verifier hachage de la partie
            if verificateur:
                try:
                    verificateur.verify()
                except ErreurHachage as e:
                    self.__logger.info("handle_put_fuuid Erreur verification hachage : %s" % str(e))
                    # Effacer le repertoire pour permettre un re-upload
                    shutil.rmtree(path_upload)
                    return web.HTTPBadRequest()

            # Verifier que la taille sur disque correspond a la taille attendue
            # Meme si le hachage est OK, s'assurer d'avoir conserve tous les bytes
            stat = path_fichier_work.stat()
            if content_length is not None and stat.st_size != content_length:
                self.__logger.info("handle_put_fuuid Erreur verification taille, sauvegarde %d, attendu %d" % (stat.st_size, content_length))
                path_fichier_work.unlink(missing_ok=True)
                return web.HTTPBadRequest()

            # Retirer le .work du fichier
            path_fichier_work.rename(path_fichier)

        self.__logger.debug("handle_put_fuuid fuuid: %s position: %s recu OK" % (fuuid, position))
        return web.HTTPOk()

    async def handle_post_fuuid(self, request: Request) -> StreamResponse:
        async with self.__connexions_write_sem:
            fuuid = request.match_info['fuuid']
            self.__logger.debug("handle_post_fuuid %s" % fuuid)

            headers = request.headers
            if request.body_exists:
                body = await request.json()
                self.__logger.debug("handle_post_fuuid body\n%s" % json.dumps(body, indent=2))
            else:
                # Aucun body - transferer le contenu du fichier sans transactions (e.g. image small)
                body = None

            # Afficher info (debug)
            self.__logger.debug("handle_post_fuuid fuuid: %s" % fuuid)
            for key, value in headers.items():
                self.__logger.debug('handle_post_fuuid key: %s, value: %s' % (key, value))

            try:
                common_name = get_common_name(request)
            except Forbidden:
                return web.HTTPForbidden()

            path_upload = self.get_path_upload_fuuid(common_name, fuuid)

            path_etat = pathlib.Path(path_upload, Constantes.FICHIER_ETAT)
            if body is not None:
                # Valider body, conserver json sur disque
                etat = body['etat']
                hachage = etat['hachage']
                with open(path_etat, 'wt') as fichier:
                    json.dump(etat, fichier)

                try:
                    transaction = body['transaction']
                    if transaction is not None:
                        await self.__etat.validateur_message.verifier(transaction)  # Lance exception si echec verification
                        path_transaction = pathlib.Path(path_upload, Constantes.FICHIER_TRANSACTION)
                        with open(path_transaction, 'wt') as fichier:
                            json.dump(transaction, fichier)
                except KeyError:
                    transaction = None

                try:
                    cles = body['cles']
                    if cles is not None:
                        await self.__etat.validateur_message.verifier(cles)  # Lance exception si echec verification
                        path_cles = pathlib.Path(path_upload, Constantes.FICHIER_CLES)
                        with open(path_cles, 'wt') as fichier:
                            json.dump(cles, fichier)
                except KeyError:
                    cles = None

            else:
                transaction = None
                cles = None
                # Sauvegarder etat.json sans body
                etat = {'hachage': fuuid, 'retryCount': 0, 'created': int(datetime.datetime.utcnow().timestamp()*1000)}
                hachage = fuuid
                with open(path_etat, 'wt') as fichier:
                    json.dump(etat, fichier)

            # Valider hachage du fichier complet (parties assemblees)
            try:
                job_valider = JobVerifierParts(transaction, path_upload, hachage, cles)
                await self.__queue_verifier_parts.put(job_valider)
                await asyncio.wait_for(job_valider.done.wait(), timeout=20)
                if job_valider.exception is not None:
                    raise job_valider.exception
            except asyncio.TimeoutError:
                self.__logger.info(
                    'handle_post_fuuid Verification fichier %s assemble en cours, repondre HTTP:201' % fuuid)
                return web.HTTPCreated()
            except Exception as e:
                self.__logger.warning('handle_post_fuuid Erreur verification hachage fichier %s assemble : %s' % (fuuid, e))
                try:
                    shutil.rmtree(path_upload)
                except FileNotFoundError:
                    self.__logger.info("Path upload %s deja supprime" % path_upload)
                return web.HTTPFailedDependency()

        return web.HTTPAccepted()

    async def handle_delete_fuuid(self, request: Request) -> StreamResponse:
        async with self.__connexions_write_sem:
            fuuid = request.match_info['fuuid']
            headers = request.headers

            # Afficher info (debug)
            self.__logger.debug("handle_delete_fuuid %s" % fuuid)

            try:
                common_name = get_common_name(request)
            except Forbidden:
                return web.HTTPForbidden()

            path_upload = self.get_path_upload_fuuid(common_name, fuuid)
            try:
                shutil.rmtree(path_upload)
            except FileNotFoundError:
                return web.HTTPNotFound()
            except Exception as e:
                self.__logger.info("handle_delete_fuuid Erreur suppression upload %s : %s" % (fuuid, e))
                return web.HTTPServerError()

        return web.HTTPOk()

    async def thread_entretien(self):
        self.__logger.debug('Entretien web')

        while not self.__stop_event.is_set():

            # TODO Entretien uploads
            await self.rapport()

            try:
                await asyncio.wait_for(self.__stop_event.wait(), 300)
            except TimeoutError:
                pass

    async def rapport(self):
        self.__logger.info("--- Rapport entretien ---")
        self.__logger.info("Semaphores read %s, write %s, backup %s",
            self.__connexions_read_sem._value, self.__connexions_write_sem._value, self.__connexions_backup_sem._value)
        self.__logger.info("URL consignation primaire : %s", self.__etat.url_consignation_primaire)
        self.__logger.info("--- Fin rapport entretien ---")

    async def thread_verifier_parts(self):
        self.__queue_verifier_parts = asyncio.Queue(maxsize=20)
        pending = [
            asyncio.create_task(self.__stop_event.wait()),
            asyncio.create_task(self.__queue_verifier_parts.get())
        ]
        while self.__stop_event.is_set() is False:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

            # Conditions de fin de thread
            if self.__stop_event.is_set() is True:
                for p in pending:
                    p.cancel()
                    try:
                        await p
                    except asyncio.CancelledError:
                        pass  # OK
                    except AttributeError:
                        pass  # Pas une task
                for d in done:
                    if d.exception():
                        raise d.exception()
                return  # Stopped

            for t in done:
                if t.exception():
                    for p in pending:
                        try:
                            p.cancel()
                            await p
                        except asyncio.CancelledError:
                            pass  # OK
                        except AttributeError:
                            pass  # Pas une task

                    raise t.exception()

            for d in done:
                if d.exception():
                    self.__logger.error("thread_verifier_parts Erreur traitement message : %s" % d.exception())
                else:
                    job_verifier_parts: JobVerifierParts = d.result()
                    try:
                        await self.traiter_job_verifier_parts(job_verifier_parts)
                    except Exception as e:
                        self.__logger.exception("thread_verifier_parts Erreur verification hachage %s" % job_verifier_parts.hachage)
                        job_verifier_parts.exception = e
                    finally:
                        # Liberer job
                        job_verifier_parts.done.set()

            if len(pending) == 0:
                raise Exception('arrete indirectement (pending vide)')

            pending.add(asyncio.create_task(self.__queue_verifier_parts.get()))

    async def traiter_job_verifier_parts(self, job: JobVerifierParts):
        try:
            path_upload = job.path_upload
            hachage = job.hachage
            args = [path_upload, hachage]
            # Utiliser thread pool pour validation
            await asyncio.to_thread(valider_hachage_upload_parts, *args)
        except Exception as e:
            self.__logger.warning(
                'traiter_job_verifier_parts Erreur verification hachage fichier %s assemble : %s' % (job.path_upload, e))
            shutil.rmtree(job.path_upload, ignore_errors=True)
            # return web.HTTPFailedDependency()
            raise e

        # Transferer vers intake
        try:
            await self.__intake.ajouter_upload(path_upload)
        except Exception as e:
            self.__logger.warning(
                'handle_post Erreur ajout fichier %s assemble au intake : %s' % (path_upload, e))
            raise e

    async def run(self, stop_event: Optional[Event] = None):
        if stop_event is not None:
            self.__stop_event = stop_event
        else:
            self.__stop_event = Event()

        runner = web.AppRunner(self.__app)
        await runner.setup()

        # Configuration du site avec SSL
        port = self.__configuration.port
        site = web.TCPSite(runner, '0.0.0.0', port, ssl_context=self.__ssl_context)

        try:
            await site.start()
            self.__logger.info("Site demarre")

            await asyncio.gather(
                self.thread_entretien(),
                self.thread_verifier_parts()
            )
        finally:
            self.__logger.info("Site arrete")
            await runner.cleanup()

    async def stream_reponse(self, request: Request, size_limit: Optional[int] = None):
        method = request.method
        fuuid = request.match_info['fuuid']
        headers = request.headers

        range_bytes = headers.get('Range')

        etag = fuuid[-16:]  # ETag requis pour caching, utiliser 16 derniers caracteres du fuuid

        # path_fichier = pathlib.Path(info.path_complet)
        # stat_fichier = path_fichier.stat()
        # taille_fichier = stat_fichier.st_size
        info_fichier = await self.__consignation.get_info_fichier(fuuid)
        if info_fichier is None or info_fichier.get('etat_fichier') == Constantes.DATABASE_ETAT_MANQUANT:
            self.__logger.debug("stream_reponse Fichier inconnu : %s" % fuuid)
            return web.HTTPNotFound()

        taille_fichier: int = info_fichier['taille']

        if size_limit is not None and taille_fichier > size_limit:
            self.__logger.error(f"stream_reponse Taille fichier {fuuid} depasse limite {size_limit}")
            return web.HTTPExpectationFailed()

        range_str = None

        headers_response = {
            'Cache-Control': 'public, max-age=604800, immutable',
            'Accept-Ranges': 'bytes',
        }

        if range_bytes is not None:
            # Calculer le content range, taille transfert
            range_parsed = parse_range(range_bytes, taille_fichier)
            start = range_parsed['start']
            end = range_parsed['end']
            taille_transfert = str(end - start + 1)
            range_str = f'bytes {start}-{end}/{taille_fichier}'
            headers_response['Content-Range'] = range_str
        else:
            # Transferer tout le contenu
            start = None
            end = None
            if taille_fichier:
                taille_transfert = str(taille_fichier)
            else:
                taille_transfert = None

        if range_str is not None:
            status = 206
        else:
            status = 200

        # Preparer reponse, headers
        response = web.StreamResponse(status=status, headers=headers_response)
        response.content_length = taille_transfert
        response.content_type = 'application/stream'
        response.etag = etag

        self.__logger.info("stream_reponse Stream fichier %s : Content-Length : %s, Content-Range: %s" % (fuuid, taille_transfert, range_str))

        await response.prepare(request)
        if method == 'HEAD':
            await response.write_eof()
            return response

        try:
            await self.__consignation.stream_fuuid(fuuid, response, start, end)
        finally:
            await response.write_eof()
        return response

    async def handle_post_backup_verifierfichiers(self, request: Request) -> StreamResponse:
        async with self.__connexions_backup_sem:
            headers = request.headers
            body = await request.json()
            uuid_backup = body['uuid_backup']
            domaine = body['domaine']
            fichiers = body['fichiers']

            # Afficher info (debug)
            self.__logger.debug("handle_post_backup_verifierfichiers %s/%s" % (uuid_backup, domaine))

            try:
                common_name = get_common_name(request)
            except Forbidden:
                return web.HTTPForbidden()

            # Creer un dict avec fichier:bool pour la reponse
            dict_fichiers = dict()
            for fichier in fichiers:
                dict_fichiers[fichier] = False

            # Parcourir les fichier dans le repertoire de backup
            path_backup = pathlib.Path(self.__etat.configuration.dir_consignation, Constantes.DIR_BACKUP)
            path_fichiers = pathlib.Path(path_backup, uuid_backup, domaine)

            try:
                for item in path_fichiers.iterdir():
                    if item.is_file() and item.name.endswith('.json.gz'):
                        dict_fichiers[item.name] = True
            except OSError as e:
                if e.errno == errno.ENOENT:
                    pass  # Ok, aucuns fichiers
                else:
                    raise e

            return web.json_response(dict_fichiers)

    async def handle_put_backup(self, request: Request) -> StreamResponse:
        async with self.__connexions_backup_sem:
            # {uuid_backup}/{domaine}/{nomfichier}
            uuid_backup = request.match_info['uuid_backup']
            domaine = request.match_info['domaine']
            nom_fichier = request.match_info['nomfichier']
            headers = request.headers

            # Afficher info (debug)
            self.__logger.debug("handle_put_backup %s/%s/%s" % (uuid_backup, domaine, nom_fichier))
            for key, value in headers.items():
                self.__logger.debug('handle_put_backup key: %s, value: %s' % (key, value))

            try:
                common_name = get_common_name(request)
            except Forbidden:
                return web.HTTPForbidden()

            # Conserver le fichier dans un tempfile
            with tempfile.TemporaryFile() as fichier_temp:
                async for chunk in request.content.iter_chunked(64*1024):
                    fichier_temp.write(chunk)

                fichier_temp.seek(0)

                with gzip.open(fichier_temp, 'r') as fichier:
                    backup = json.load(fichier)

                try:
                    enveloppe = await self.__etat.validateur_message.verifier(backup)
                    if ConstantesMillegrilles.SECURITE_SECURE not in enveloppe.get_exchanges:
                        raise Exception('Fichier ne backup signe par mauvais certificat (doit etre 4.secure)')
                except Exception:
                    self.__logger.info("Erreur validation fichier de backup %s - SKIP" % nom_fichier)
                    return web.HTTPBadRequest()

                # Consigner le fichier de backup
                await self.__consignation.conserver_backup(fichier_temp, uuid_backup, domaine, nom_fichier)

            return web.HTTPOk()

    async def handle_get_backup(self, request: Request) -> StreamResponse:
        async with self.__connexions_backup_sem:
            uuid_backup: str = request.match_info['uuid_backup']
            domaine: str = request.match_info['domaine']
            fichier_nom: str = request.match_info['nomfichier']

            try:
                common_name = get_common_name(request)
            except Forbidden:
                return web.HTTPForbidden()

            # Verifier si le fichier existe
            try:
                info = await self.__consignation.get_info_fichier_backup(uuid_backup, domaine, fichier_nom)
            except FileNotFoundError:
                return web.HTTPNotFound()

            headers_response = {
                'Cache-Control': 'public, max-age=604800, immutable',
            }
            response = web.StreamResponse(status=200, headers=headers_response)
            response.content_length = info['taille']
            response.content_type = 'application/gzip'
            await response.prepare(request)
            await self.__consignation.stream_backup(response, uuid_backup, domaine, fichier_nom)
            await response.write_eof()
        return response

    async def handle_get_fichier_sync(self, request: Request) -> StreamResponse:
        async with self.__connexions_backup_sem:
            fichier_nom: str = request.match_info['fichier']
            headers = request.headers

            if fichier_nom not in CONST_FICHIERS_ACCEPTES_SYNC:
                self.__logger.debug("handle_get_fichier_sync Fichier %s non supporte" % fichier_nom)
                return web.HTTPNotFound()

            # Afficher info (debug)
            self.__logger.debug("handle_get_fichier_sync %s" % fichier_nom)
            for key, value in headers.items():
                self.__logger.debug('handle_get_fichier_sync key: %s, value: %s' % (key, value))

            try:
                common_name = get_common_name(request)
            except Forbidden:
                return web.HTTPForbidden()

            path_data = pathlib.Path(self.__etat.configuration.dir_data)
            path_fichier = pathlib.Path(path_data, fichier_nom)
            try:
                stat_fichier = path_fichier.stat()
            except OSError as e:
                if e.errno == errno.ENOENT:
                    return web.HTTPNotFound()
                else:
                    self.__logger.exception("Erreur chargement fichier %s" % fichier_nom)
                    return web.HTTPServerError()

            headers_response = {
                'Cache-Control': 'no-store',
            }

            response = web.StreamResponse(status=200, headers=headers_response)
            response.content_length = stat_fichier.st_size

            if fichier_nom.endswith('.gz'):
                response.content_type = 'application/gzip'
            elif fichier_nom.endswith('.jsonl'):
                response.content_type = 'application/jsonl'
            else:
                response.content_type = 'application/stream'

            # Repondre avec le fichier
            await response.prepare(request)
            with open(path_fichier, 'rb') as fichier:
                while True:
                    chunk = fichier.read(64*1024)
                    if not chunk:
                        break
                    await response.write(chunk)

            await response.write_eof()
        return response

    # *********
    # Backup V2
    # *********

    async def handle_put_backup_v2(self, request: Request) -> StreamResponse:
        async with self.__connexions_backup_sem:
            # /fichiers_transfert/backup_v2/{domaine}/{type_fichier}/{nomfichier}  -> final
            # /fichiers_transfert/backup_v2/{domaine}/{type_fichier}/{version}/{nomfichier}  -> concatene ou incremental
            domaine = request.match_info['domaine']
            type_fichier = request.match_info['type_fichier']
            try:
                version = request.match_info['version']
            except KeyError:
                version = None
            nom_fichier: str = request.match_info['nomfichier']

            self.__logger.debug("handle_put_backup_v2 %s" % nom_fichier)
            error = self._verifier_domaine_backup_v2(request)
            if error:
                return error

            if type_fichier not in ['final', 'concatene', 'incremental']:
                self.__logger.error("handle_put_backup_v2 Type de fichier non supporte : %s" % type_fichier)
                return web.HTTPBadRequest()

            self.__logger.debug("handle_put_backup_v2 %s/%s/%s/%s" % (domaine, type_fichier, version, nom_fichier))
            suffix_digest_fichier = nom_fichier.split('.')[0].split('_').pop()

            # Conserver le fichier dans un tempfile
            hacheur = Hacheur('blake2b-512', 'base58btc')
            with tempfile.TemporaryFile() as fichier_temp:
                async for chunk in request.content.iter_chunked(64*1024):
                    fichier_temp.write(chunk)
                    hacheur.update(chunk)

                resultat_hachage = hacheur.finalize()
                self.__logger.debug("handle_put_backup_v2 Resultat hachage fichier %s = %s" % (nom_fichier, resultat_hachage))
                if resultat_hachage.endswith(suffix_digest_fichier) is False:
                    self.__logger.error("handle_put_backup_v2 Digest du fichier mismatch : %s" % nom_fichier)
                    return web.HTTPBadRequest()

                fichier_temp.seek(0)

                # Lire le header du fichier de backup
                header_archive = lire_header_archive_backup(fichier_temp)
                if domaine != header_archive['domaine']:
                    self.__logger.error("handle_put_backup_v2 Mismatch de domaine fourni %s et celui du header : %s" % (domaine, nom_fichier))
                    return web.HTTPBadRequest()

                # Confirmer le type et domaine
                char_type_archive = header_archive['type_archive']
                if type_fichier == 'final' and char_type_archive != 'F':
                    self.__logger.error("handle_put_backup_v2 Type de fichier doit etre F pour final : %s" % nom_fichier)
                    return web.HTTPBadRequest()
                elif type_fichier == 'concatene' and char_type_archive != 'C':
                    self.__logger.error("handle_put_backup_v2 Type de fichier doit etre C pour final : %s" % nom_fichier)
                    return web.HTTPBadRequest()
                elif type_fichier == 'incremental' and char_type_archive != 'I':
                    self.__logger.error("handle_put_backup_v2 Type de fichier doit etre I pour final : %s" % nom_fichier)
                    return web.HTTPBadRequest()

                # Consigner le fichier de backup
                await self.__consignation.put_backup_v2_fichier(fichier_temp, domaine, nom_fichier, type_fichier, version)

        return web.HTTPOk()

    async def handle_get_backup_v2(self, request: Request) -> StreamResponse:
        async with self.__connexions_backup_sem:
            domaine: str = request.match_info['domaine']
            fichier_nom: str = request.match_info['nomfichier']
            try:
                version = request.match_info['version']
            except KeyError:
                version = None

            self.__logger.debug("handle_get_backup_v2 %s/%s/%s" % (domaine, version, fichier_nom))
            error = self._verifier_domaine_backup_v2(request)
            if error:
                return error

            stream = None
            try:
                stream = await self.__consignation.get_backup_v2_fichier_stream(domaine, fichier_nom, version)
                headers_response = {
                    # 'Cache-Control': 'public, max-age=604800, immutable',
                }
                response = web.StreamResponse(status=200, headers=headers_response)
                # response.content_length = info['taille']
                response.content_type = 'application/octet-stream'
                await response.prepare(request)
                while True:
                    chunk = stream.read(64 * 1024)
                    if not chunk:
                        break
                    await response.write(chunk)
                await response.write_eof()
                return response
            except FileNotFoundError:
                return web.HTTPNotFound()
            finally:
                if stream is not None:
                    stream.close()

    async def handle_get_backup_v2_liste_domaines(self, request: Request) -> StreamResponse:
        async with self.__connexions_backup_sem:
            self.__logger.debug("handle_get_backup_v2_liste_domaines")

            # Charger la liste des domaines
            domaines = await self.__consignation.get_backup_v2_domaines()

            return web.json_response({'domaines': domaines})

    async def handle_get_backup_v2_liste_versions(self, request: Request) -> StreamResponse:
        async with self.__connexions_backup_sem:
            domaine: str = request.match_info['domaine']
            self.__logger.debug("handle_get_backup_v2_liste_versions domaine %s" % domaine)

            error = self._verifier_domaine_backup_v2(request)
            if error:
                return error

            # Charger la liste de fichiers du repertoire de backup v2 pour le domaine
            versions = await self.__consignation.get_backup_v2_versions(domaine)

            return web.json_response(versions)

    async def handle_get_backup_v2_liste_archives(self, request: Request) -> Optional[StreamResponse]:
        async with self.__connexions_backup_sem:
            domaine: str = request.match_info['domaine']
            try:
                version: Optional[str] = request.match_info['version']
            except KeyError:
                version = None

            self.__logger.debug("handle_get_backup_v2_liste_archives domaine %s, version %s" % (domaine, version))
            error = self._verifier_domaine_backup_v2(request)
            if error:
                return error

            # Charger la liste de fichiers du repertoire de backup v2 pour le domaine
            liste_fichiers = await self.__consignation.get_backup_v2_list(domaine, version)

            headers_response = {
                # 'Cache-Control': 'public, max-age=604800, immutable',
            }
            response = web.StreamResponse(status=200, headers=headers_response)
            await response.prepare(request)

            newline = "\n".encode('utf-8')
            for f in liste_fichiers:
                await response.write(f.encode('utf-8') + newline)

            await response.write_eof()
        return response

    def _verifier_domaine_backup_v2(self, request) -> Optional[HTTPClientError]:
        domaine: str = request.match_info['domaine']
        self.__logger.debug("_verifier_domaine_backup_v2 domaine: %s" % domaine)

        # Charger certificat peer du transport aiohttp
        peercert_dict = request.get_extra_info('peercert')
        subject_info = [v[0] for v in peercert_dict['subject']]
        cert_ou = [v[1] for v in subject_info if v[0] == 'organizationalUnitName'].pop()

        if cert_ou == 'nginx':
            # Le certificat est celui du reverse-proxy (nginx)
            if request.headers['VERIFIED'] != 'SUCCESS':
                self.__logger.error("_verifier_domaine_backup_v2 ssl VERIFIED != SUCCESS pour requete {}", request.url)
                return web.HTTPForbidden()

            # Verifier le certificat client externe qui a deja ete verifie par nginx
            try:
                cert_pem = unquote(request.headers['X-SSL-CERT'])
                enveloppe = EnveloppeCertificat.from_pem(cert_pem)
            except KeyError:
                self.__logger.error("_verifier_domaine_backup_v2 Certificat absent de la requete {}", request.url)
                return web.HTTPForbidden()

        else:
            # Connexion interne - recuperer certificat valide via aiohttp
            try:
                ssl_object = request.get_extra_info('ssl_object')
                peercert_der = ssl_object.getpeercert(binary_form=True)
                enveloppe = EnveloppeCertificat.from_der(peercert_der)
            except:
                self.__logger.exception("Erreur chargement certificat SSL connexion interne")
                return web.HTTPForbidden()

        try:
            if ConstantesMillegrilles.SECURITE_PROTEGE in enveloppe.get_exchanges:
                # S'assurer que le certificat client correspond au bon domaine et a le niveau de securite 3.protege
                if domaine not in enveloppe.get_domaines:
                    self.__logger.error("_verifier_domaine_backup_v2 Certificat n'a pas le domaine demande {}", request.url)
                    return web.HTTPForbidden()
            elif ConstantesMillegrilles.SECURITE_PRIVE in enveloppe.get_exchanges:
                # Prive -> consignation fichiers pour backup secondaires
                if 'fichiers' not in enveloppe.get_roles:
                    self.__logger.error("_verifier_domaine_backup_v2 Certificat 2.prive n'a pas le role fichiers {}", request.url)
                    return web.HTTPForbidden()
            else:
                self.__logger.error("_verifier_domaine_backup_v2 Certificat n'a pas la securite 3.protege", request.url)
                return web.HTTPForbidden()
        except ExtensionNotFound:
            # Extension manquante (get_domaines ou get_roles)
            return web.HTTPForbidden()

        return None


def parse_range(range, taille_totale):
    re_compiled = re.compile('bytes=([0-9]*)\\-([0-9]*)?')
    m = re_compiled.search(range)

    start = m.group(1)
    if start is not None:
        start = int(start)
    else:
        start = 0

    end = m.group(2)
    if end is None:
        end = taille_totale - 1
    else:
        try:
            end = int(end)
            if end > taille_totale:
                end = taille_totale - 1
        except ValueError:
            end = taille_totale - 1

    result = {
        'start': start,
        'end': end,
    }

    return result


def extract_subject(dn: str):
    cert_subject = dict()
    for e in dn.split(','):
        key, value = e.split('=')
        cert_subject[key] = value

    return cert_subject


def valider_hachage_upload_parts(path_upload: pathlib.Path, hachage: str):
    positions = list()
    for item in path_upload.iterdir():
        if item.is_file():
            nom_fichier = str(item)
            if nom_fichier.endswith('.part'):
                position = int(item.name.split('.')[0])
                positions.append(position)
    positions = sorted(positions)

    verificateur = VerificateurHachage(hachage)

    for position in positions:
        path_fichier = pathlib.Path(path_upload, '%d.part' % position)

        with open(path_fichier, 'rb') as fichier:
            while True:
                chunk = fichier.read(64*1024)
                if not chunk:
                    break
                verificateur.update(chunk)

    verificateur.verify()  # Lance une exception si le hachage est incorrect


def get_common_name(request: Request):
    headers = request.headers
    peercert = request.get_extra_info('peercert')
    subject_info = [v[0] for v in peercert['subject']]
    cert_ou = [v[1] for v in subject_info if v[0] == 'organizationalUnitName'].pop()
    common_name = [v[1] for v in subject_info if v[0] == 'commonName'].pop()

    if cert_ou == 'nginx':
        if headers.get('VERIFIED') == 'SUCCESS':
            # Utiliser les headers fournis par nginx
            cert_subject = extract_subject(headers.get('DN'))
            common_name = cert_subject['CN']
        elif headers.get('VERIFIED') == 'NONE':
            # Flag NONE - on utilise le header X-User-Id
            try:
                common_name = headers['X-User-Id']
            except KeyError:
                raise Forbidden()
        else:
            raise Forbidden()
    else:
        # Connexion interne - on ne peut pas verifier que le certificat a l'exchange secure, mais la
        # connexion est directe (interne au VPN docker) et il est presentement valide.
        pass

    return common_name


class Forbidden(Exception):
    pass
