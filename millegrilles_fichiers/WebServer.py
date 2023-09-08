import asyncio
import logging
import json
import shutil

import pathlib
import re

from aiohttp import web
from aiohttp.web_request import Request, StreamResponse
from asyncio import Event
from asyncio.exceptions import TimeoutError
from ssl import SSLContext
from typing import Optional, Union

from millegrilles_messages.messages import Constantes as ConstantesMillegrilles
from millegrilles_messages.messages.Hachage import VerificateurHachage, ErreurHachage

from millegrilles_fichiers import Constantes
from millegrilles_fichiers.Configuration import ConfigurationWeb
from millegrilles_fichiers.Consignation import InformationFuuid
from millegrilles_fichiers.EtatFichiers import EtatFichiers
from millegrilles_fichiers.Commandes import CommandHandler
from millegrilles_fichiers.Intake import IntakeStreaming


class JobVerifierParts:

    def __init__(self, path_upload: pathlib.Path, hachage: str):
        self.path_upload = path_upload
        self.hachage = hachage
        self.done = asyncio.Event()
        self.valide: Optional[bool] = None
        self.exception: Optional[Exception] = None


class WebServer:

    def __init__(self, etat: EtatFichiers, commandes: CommandHandler, intake: IntakeStreaming):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__etat = etat
        self.__commandes = commandes
        self.__intake = intake

        self.__app = web.Application()
        self.__stop_event: Optional[Event] = None
        self.__configuration = ConfigurationWeb()
        self.__ssl_context: Optional[SSLContext] = None

        self.__queue_verifier_parts: Optional[asyncio.Queue] = None

    def setup(self, configuration: Optional[dict] = None):
        self._charger_configuration(configuration)
        self._preparer_routes()
        self._charger_ssl()

    def _charger_configuration(self, configuration: Optional[dict] = None):
        self.__configuration.parse_config(configuration)

    def get_path_upload_fuuid(self, cn: str, fuuid: str):
        return pathlib.Path(self.__etat.configuration.dir_consignation, Constantes.DIR_STAGING_UPLOAD, cn, fuuid)

    def _preparer_routes(self):
        self.__app.add_routes([
            # /fichiers_transfert
            web.get('/fichiers_transfert/{fuuid}', self.handle_get_fuuid),
            web.put('/fichiers_transfert/{fuuid}/{position}', self.handle_put_fuuid),
            web.post('/fichiers_transfert/{fuuid}', self.handle_post_fuuid),
            web.delete('/fichiers_transfert/{fuuid}', self.handle_delete_fuuid),

            # /sync
            #   route.get('/fuuidsLocaux.txt.gz', (req, res, next) => getFichier(req, res, next, PATH_FUUIDS_LOCAUX))
            #   route.get('/fuuidsArchives.txt.gz', (req, res, next) => getFichier(req, res, next, PATH_FUUIDS_ARCHIVES))
            #   route.get('/fuuidsManquants.txt.gz', (req, res, next) => getFichier(req, res, next, PATH_FUUIDS_MANQUANTS))
            #   route.get('/listingBackup.txt.gz', (req, res, next) => getFichier(req, res, next, PATH_LISTING_BACKUP))
            #   route.get('/fuuidsNouveaux.txt', (req, res, next) => getFichier(req, res, next, PATH_FUUIDS_NOUVEAUX, {gzip: false}))
            #   route.post('/fuuidsInfo', express.json(), getFuuidsInfo)

            # /backup
            #   route.post('/verifierFichiers', express.json(), verifierBackup)
            #   route.put('/upload/:uuid_backup/:domaine/:nomfichier', recevoirFichier)
            #   route.get('/download/:uuid_backup/:domaine/:nomfichier', downloaderFichier)
        ])

    def _charger_ssl(self):
        self.__ssl_context = SSLContext()
        self.__logger.debug("Charger certificat %s" % self.__configuration.web_cert_pem_path)
        self.__ssl_context.load_cert_chain(self.__configuration.web_cert_pem_path,
                                           self.__configuration.web_key_pem_path)
        self.__ssl_context.load_verify_locations(cafile=self.__configuration.ca_pem_path)

    async def handle_get_fuuid(self, request: Request):
        fuuid = request.match_info['fuuid']
        method = request.method
        headers = request.headers

        self.__logger.debug("handle_get_fuuid method %s, fuuid %s" % (method, fuuid))

        # TODO Fix me
        return web.HTTPInternalServerError()

    async def handle_put_fuuid(self, request: Request) -> StreamResponse:
        fuuid = request.match_info['fuuid']
        position = request.match_info['position']
        headers = request.headers

        # Afficher info (debug)
        self.__logger.debug("handle_put_fuuid fuuid: %s position: %s" % (fuuid, position))
        for key, value in headers.items():
            self.__logger.debug('handle_put_fuuid key: %s, value: %s' % (key, value))

        if headers.get('VERIFIED') != 'SUCCESS':
            return web.HTTPForbidden()

        cert_subject = extract_subject(headers.get('DN'))
        common_name = cert_subject['CN']

        content_hash = headers.get('x-content-hash')
        content_length = int(headers['Content-Length'])

        # Creer repertoire pour sauvegader la partie de fichier
        path_upload = self.get_path_upload_fuuid(common_name, fuuid)
        path_upload.mkdir(parents=True, exist_ok=True)

        path_fichier = pathlib.Path(path_upload, '%s.part' % position)
        self.__logger.debug("handle_put_fuuid Conserver part %s" % path_fichier)

        verificateur = VerificateurHachage(content_hash)
        with open(path_fichier, 'wb') as fichier:
            async for chunk in request.content.iter_chunked(64 * 1024):
                verificateur.update(chunk)
                fichier.write(chunk)

        # Verifier hachage de la partie
        try:
            verificateur.verify()
        except ErreurHachage as e:
            self.__logger.info("handle_put_fuuid Erreur verification hachage : %s" % str(e))
            path_fichier.unlink(missing_ok=True)
            return web.HTTPBadRequest()

        # Verifier que la taille sur disque correspond a la taille attendue
        # Meme si le hachage est OK, s'assurer d'avoir conserve tous les bytes
        stat = path_fichier.stat()
        if stat.st_size != content_length:
            self.__logger.info("handle_put_fuuid Erreur verification taille, sauvegarde %d, attendu %d" % (stat.st_size, content_length))
            path_fichier.unlink(missing_ok=True)
            return web.HTTPBadRequest()

        self.__logger.debug("handle_put_fuuid fuuid: %s position: %s recu OK" % (fuuid, position))

        return web.HTTPOk()

    async def handle_post_fuuid(self, request: Request) -> StreamResponse:
        fuuid = request.match_info['fuuid']
        self.__logger.debug("handle_post_fuuid %s" % fuuid)

        headers = request.headers
        body = await request.json()

        # Afficher info (debug)
        self.__logger.debug("handle_post_fuuid fuuid: %s" % fuuid)
        for key, value in headers.items():
            self.__logger.debug('handle_post_fuuid key: %s, value: %s' % (key, value))
        self.__logger.debug("handle_post_fuuid body\n%s" % json.dumps(body, indent=2))

        if headers.get('VERIFIED') != 'SUCCESS':
            return web.HTTPForbidden()

        cert_subject = extract_subject(headers.get('DN'))
        common_name = cert_subject['CN']

        path_upload = self.get_path_upload_fuuid(common_name, fuuid)

        # Valider body, conserver json sur disque
        path_etat = pathlib.Path(path_upload, Constantes.FICHIER_ETAT)
        etat = body['etat']
        hachage = etat['hachage']
        with open(path_etat, 'wt') as fichier:
            json.dump(etat, fichier)

        try:
            transaction = body['transaction']
            await self.__etat.validateur_message.verifier(transaction)  # Lance exception si echec verification
            path_transaction = pathlib.Path(path_upload, Constantes.FICHIER_TRANSACTION)
            with open(path_transaction, 'wt') as fichier:
                json.dump(transaction, fichier)
        except KeyError:
            pass

        try:
            cles = body['cles']
            await self.__etat.validateur_message.verifier(cles)  # Lance exception si echec verification
            path_cles = pathlib.Path(path_upload, Constantes.FICHIER_CLES)
            with open(path_cles, 'wt') as fichier:
                json.dump(cles, fichier)
        except KeyError:
            pass

        # Valider hachage du fichier complet (parties assemblees)
        try:
            job_valider = JobVerifierParts(path_upload, hachage)
            await self.__queue_verifier_parts.put(job_valider)
            await asyncio.wait_for(job_valider.done.wait(), timeout=270)
            if job_valider.exception is not None:
                raise job_valider.exception
        except Exception as e:
            self.__logger.warning('handle_post_fuuid Erreur verification hachage fichier %s assemble : %s' % (fuuid, e))
            shutil.rmtree(path_upload)
            return web.HTTPFailedDependency()

        # Transferer vers intake
        try:
            await self.__intake.ajouter_upload(path_upload)
        except Exception as e:
            self.__logger.warning('handle_post_fuuid Erreur ajout fichier %s assemble au intake : %s' % (fuuid, e))
            shutil.rmtree(path_upload)
            return web.HTTPServerError()

        return web.HTTPAccepted()

    async def handle_delete_fuuid(self, request: Request) -> StreamResponse:
        fuuid = request.match_info['fuuid']
        headers = request.headers

        # Afficher info (debug)
        self.__logger.debug("handle_delete_fuuid %s" % fuuid)

        if headers.get('VERIFIED') != 'SUCCESS':
            return web.HTTPForbidden()

        cert_subject = extract_subject(headers.get('DN'))
        common_name = cert_subject['CN']

        path_upload = self.get_path_upload_fuuid(common_name, fuuid)
        try:
            shutil.rmtree(path_upload)
        except FileNotFoundError:
            return web.HTTPNotFound()
        except Exception as e:
            self.__logger.info("handle_delete_fuuid Erreur suppression upload %s : %s" % (fuuid, e))
            return web.HTTPServerError()

        return web.HTTPOk()

    # async def handle_path_fuuid(self, request: Request):
    #     fuuid = request.match_info['fuuid']
    #     headers = request.headers
    #     jwt_token = request.query.get('jwt')
    #
    #     if jwt_token is None:
    #         # Token JWT absent
    #         self.__logger.debug("handle_path_fuuid ERROR jwt absent pour requete sur fuuid %s" % fuuid)
    #         return web.HTTPForbidden()
    #
    #     self.__logger.debug("handle_path_fuuid Requete sur fuuid %s" % fuuid)
    #     try:
    #         claims = await self.verifier_token_jwt(jwt_token, fuuid)
    #         if claims is False:
    #             self.__logger.debug("handle_path_fuuid ERROR jwt refuse pour requete sur fuuid %s" % fuuid)
    #             return web.HTTPUnauthorized()
    #
    #         # Verifier si le fichier existe deja (dechiffre)
    #         reponse = await self.__commandes.traiter_fuuid(fuuid, jwt_token, claims)
    #         if reponse is None:
    #             # On n'a aucune information sur ce fichier/download.
    #             self.__logger.warning("handle_path_fuuid Aucune information sur le download %s", fuuid)
    #             return web.HTTPInternalServerError()
    #
    #         if reponse.status == 404:
    #             # Fichier inconnu localement
    #             return web.HTTPNotFound
    #         elif reponse.status is not None and reponse.status != 200:
    #             # On a une erreur du back-end (consignation)
    #             return web.HTTPInternalServerError()
    #
    #         if reponse.est_pret:
    #             # Repondre avec le stream demande
    #             return await self.stream_reponse(request, reponse)
    #
    #         # HTTP 204 - le contenu n'est pas pret
    #         if reponse.position_courante is not None:
    #             headers_response = {
    #                 'Content-Type': reponse.mimetype,
    #                 'X-File-Size': str(reponse.taille),
    #                 'X-File-Position': str(reponse.position_courante),
    #             }
    #             return web.Response(status=204, headers=headers_response)
    #
    #         return web.HTTPInternalServerError()  # Fix me
    #
    #     except Exception:
    #         self.__logger.exception("handle_path_fuuid ERROR")
    #         return web.HTTPInternalServerError()

    async def thread_entretien(self):
        self.__logger.debug('Entretien web')

        while not self.__stop_event.is_set():

            # TODO Entretien uploads

            try:
                await asyncio.wait_for(self.__stop_event.wait(), 30)
            except TimeoutError:
                pass

    async def thread_verifier_parts(self):
        self.__queue_verifier_parts = asyncio.Queue(maxsize=20)
        pending = [self.__stop_event.wait(), self.__queue_verifier_parts.get()]
        while self.__stop_event.is_set() is False:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

            # Conditions de fin de thread
            if self.__stop_event.is_set() is True:
                return  # Stopped

            for t in done:
                if t.exception():
                    raise t.exception()

            job_verifier_parts: JobVerifierParts = done.pop().result()
            try:
                path_upload = job_verifier_parts.path_upload
                hachage = job_verifier_parts.hachage
                args = [path_upload, hachage]
                # Utiliser thread pool pour validation
                await asyncio.to_thread(valider_hachage_upload_parts, *args)
            except Exception as e:
                self.__logger.exception("thread_verifier_parts Erreur verification hachage %s" % job_verifier_parts.hachage)
                job_verifier_parts.exception = e

            # Liberer job
            job_verifier_parts.done.set()

            pending.add(self.__queue_verifier_parts.get())

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

    async def stream_reponse(self, request, info: InformationFuuid):
        range_bytes = request.headers.get('Range')

        fuuid = info.fuuid
        etag = fuuid[-16:]  # ETag requis pour caching, utiliser 16 derniers caracteres du fuuid

        path_fichier = pathlib.Path(info.path_complet)
        stat_fichier = path_fichier.stat()
        taille_fichier = stat_fichier.st_size

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
            range_str = f'bytes {start}-{end}/{taille_fichier}'
            headers_response['Content-Range'] = range_str
            taille_transfert = str(end - start + 1)
        else:
            # Transferer tout le contenu
            start = None
            end = None
            taille_transfert = str(taille_fichier)

        if range_str is not None:
            status = 206
        else:
            status = 200

        # Preparer reponse, headers
        response = web.StreamResponse(status=status, headers=headers_response)
        response.content_length = taille_transfert
        response.content_type = info.mimetype
        response.etag = etag

        await response.prepare(request)
        with path_fichier.open(mode='rb') as input_file:
            if start is not None and start > 0:
                input_file.seek(start, 0)
                position = start
            else:
                position = 0
            for chunk in input_file:
                if end is not None and position + len(chunk) > end:
                    taille_chunk = end - position + 1
                    await response.write(chunk[:taille_chunk])
                    break  # Termine
                else:
                    await response.write(chunk)
                position += len(chunk)

        await response.write_eof()


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
        end = int(end)
        if end > taille_totale:
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
