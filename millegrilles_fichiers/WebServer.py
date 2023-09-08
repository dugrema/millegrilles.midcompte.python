import asyncio
import logging
import jwt
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


class WebServer:

    def __init__(self, etat: EtatFichiers, commandes: CommandHandler):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__etat = etat
        self.__commandes = commandes

        self.__app = web.Application()
        self.__stop_event: Optional[Event] = None
        self.__configuration = ConfigurationWeb()
        self.__ssl_context: Optional[SSLContext] = None

    def setup(self, configuration: Optional[dict] = None):
        self._charger_configuration(configuration)
        self._preparer_routes()
        self._charger_ssl()

    def _charger_configuration(self, configuration: Optional[dict] = None):
        self.__configuration.parse_config(configuration)

    def get_path_upload_fuuid(self, cn: str, fuuid: str):
        return pathlib.Path(self.__etat.configuration.dir_consignation, Constantes.DIR_STAGING_UPLOAD, cn, fuuid)

    def get_path_intake_fuuid(self, cn: str, fuuid: str):
        return pathlib.Path(self.__etat.configuration.dir_consignation, Constantes.DIR_STAGING_INTAKE, cn, fuuid)

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
        headers = request.headers

        self.__logger.debug("handle_post_fuuid %s" % fuuid)

        # TODO Fix me
        return web.HTTPInternalServerError()

    async def handle_delete_fuuid(self, request: Request) -> StreamResponse:
        fuuid = request.match_info['fuuid']
        headers = request.headers

        self.__logger.debug("handle_delete_fuuid %s" % fuuid)

        # TODO Fix me
        return web.HTTPInternalServerError()

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

    async def entretien(self):
        self.__logger.debug('Entretien web')

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

            while not self.__stop_event.is_set():
                await self.entretien()
                try:
                    await asyncio.wait_for(self.__stop_event.wait(), 30)
                except TimeoutError:
                    pass
        finally:
            self.__logger.info("Site arrete")
            await runner.cleanup()

    async def verifier_token_jwt(self, token: str, fuuid: str) -> Union[bool, dict]:
        # Recuperer kid, charger certificat pour validation
        header = jwt.get_unverified_header(token)
        fingerprint = header['kid']
        enveloppe = await self.__etat.charger_certificat(fingerprint)

        # roles = enveloppe.get_roles
        # if 'collections' in roles:  # Note - corriger, les JWT devraient etre generes par un domaine
        #     pass  # Ok
        # else:
        #     # Certificat n'est pas autorise a signer des streams
        #     return False

        domaines = enveloppe.get_domaines
        if 'GrosFichiers' in domaines or 'Messagerie' in domaines:
            pass  # OK
        else:
            # Certificat n'est pas autorise a signer des streams
            self.__logger.warning("Certificat de mauvais domaine pour JWT (doit etre GrosFichiers,Messagerie)")
            return False

        exchanges = enveloppe.get_exchanges
        if ConstantesMillegrilles.SECURITE_SECURE not in exchanges:
            # Certificat n'est pas autorise a signer des streams
            self.__logger.warning("Certificat de mauvais niveau de securite pour JWT (doit etre 4.secure)")
            return False

        public_key = enveloppe.get_public_key()

        try:
            claims = jwt.decode(token, public_key, algorithms=['EdDSA'])
        except jwt.exceptions.InvalidSignatureError:
            # Signature invalide
            return False

        self.__logger.debug("JWT claims pour %s = %s" % (fuuid, claims))

        if claims['sub'] != fuuid:
            # JWT pour le mauvais fuuid
            return False

        return claims

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