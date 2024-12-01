import asyncio
import logging
import json

import aiohttp
import jwt
import pathlib
import re

from aiohttp import web
from aiohttp.web_request import Request
from ssl import SSLContext
from typing import Optional, Union

from gridfs.errors import FileExists

from millegrilles_messages.messages import Constantes

from millegrilles_streaming.StreamingManager import StreamingManager
from millegrilles_streaming.Structs import InformationFuuid


class WebServer:

    def __init__(self, manager: StreamingManager):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__manager = manager
        self.__app = web.Application()
        self.__ssl_context: Optional[SSLContext] = None

    async def setup(self):
        await asyncio.to_thread(self._charger_configuration)
        self._preparer_routes()
        self._charger_ssl()
        # self.__webrunner = WebRunner(self.context, self.__app, ipv6=self.__ipv6)

    def _charger_configuration(self):
        self.__manager.context.configuration.parse_config()

    def _preparer_routes(self):
        self.__app.add_routes([
            web.get('/stream_transfert/{fuuid}', self.handle_path_fuuid),
            web.get('/streams/{fuuid}', self.handle_path_fuuid),
        ])

    def _charger_ssl(self):
        configuration = self.__manager.context.configuration
        self.__ssl_context = SSLContext()
        self.__logger.debug("Charger certificat %s" % configuration.cert_path)
        self.__ssl_context.load_cert_chain(str(configuration.cert_path),
                                           str(configuration.key_path))

    async def handle_path_fuuid(self, request: Request):
        fuuid = request.match_info['fuuid']
        headers = request.headers
        jwt_token = request.query.get('jwt')

        if jwt_token is None:
            # Token JWT absent
            self.__logger.debug("handle_path_fuuid ERROR jwt absent pour requete sur fuuid %s" % fuuid)
            return web.HTTPForbidden()

        header_range = headers.get('Range')
        self.__logger.debug("handle_path_fuuid Requete sur fuuid %s : %s" % (fuuid, header_range))
        try:
            claims = await self.verifier_token_jwt(jwt_token, fuuid)
            if claims is False:
                self.__logger.debug("handle_path_fuuid ERROR jwt refuse pour requete sur fuuid %s" % fuuid)
                return web.HTTPUnauthorized()

            # Check if the decrypted file is already available
            file = InformationFuuid(fuuid, jwt_token, claims)
            try:
                await self.__manager.load_decrypted_information(file)
                return await self.stream_reponse(request, file)
            except FileNotFoundError:
                pass  # File not already decrypted, add job

            try:
                job = await self.__manager.add_job(file)
                if job.status_code is not None and job.status_code != 200:
                    # On a une erreur du back-end (consignation)
                    return web.HTTPInternalServerError()
            except aiohttp.ClientResponseError as e:
                if e.status == 404:
                    return web.HTTPNotFound()
                elif e.status == 403:
                    return web.HTTPForbidden()
                else:
                    raise e
            except FileNotFoundError:
                return web.HTTPNotFound()
            except FileExistsError:
                # File was just made available
                return await self.stream_reponse(request, file)

            if job.ready_event.is_set():  # Job done, respond with stream
                return await self.stream_reponse(request, job.file_information)

            # HTTP 204 - le contenu n'est pas pret
            if job.file_position is not None:
                info = job.file_information
                headers_response = {
                    'Content-Type': info.mimetype,
                    'X-File-Size': str(job.file_size),
                    'X-File-Position': str(job.file_position),
                }
                return web.Response(status=204, headers=headers_response)

            return web.HTTPInternalServerError()

        except Exception:
            self.__logger.exception("handle_path_fuuid ERROR")
            return web.HTTPInternalServerError()

    async def run(self):
        await self.__run_app()

    async def __run_app(self):
        runner = web.AppRunner(self.__app)
        await runner.setup()

        # Configuration du site avec SSL
        port = self.__manager.context.configuration.web_port
        site = web.TCPSite(runner, '0.0.0.0', port, ssl_context=self.__ssl_context)

        try:
            await site.start()
            self.__logger.info("Website started on port %d" % port)
            await self.__manager.context.wait()  # Wait until application shuts down
        except:
            self.__logger.exception("Error running website - quitting")
            self.__manager.context.stop()
        finally:
            self.__logger.info("Website stopped")
            await runner.cleanup()

    async def verifier_token_jwt(self, token: str, fuuid: str) -> Union[bool, dict]:
        # Recuperer kid, charger certificat pour validation
        header = jwt.get_unverified_header(token)
        fingerprint = header['kid']
        producer = await self.__manager.context.get_producer()
        enveloppe = await producer.fetch_certificate(fingerprint)

        domaines = enveloppe.get_domaines
        if 'GrosFichiers' in domaines:
            pass  # OK
        else:
            # Certificat n'est pas autorise a signer des streams
            self.__logger.warning("Certificat de mauvais domaine pour JWT (doit etre GrosFichiers,Messagerie)")
            return False

        exchanges = enveloppe.get_exchanges
        if Constantes.SECURITE_SECURE not in exchanges:
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

        # Touch file - indicates it is still active (prevents cleanup process from deleting it)
        await asyncio.to_thread(path_fichier.touch)

        range_str = None

        headers_response = {
            'Cache-Control': 'public, max-age=604800, immutable',
            'Accept-Ranges': 'bytes',
        }

        # Try to load mimetype from json info file
        mimetype = info.mimetype
        if mimetype is None:
            path_json = pathlib.Path(path_fichier.parent, f'{fuuid}.json')
            try:
                with open(path_json, 'rt') as fp:
                    info = await asyncio.to_thread(json.load, fp)
                mimetype = info['mimetype']
            except (FileNotFoundError, json.JSONDecodeError):
                mimetype = None

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
        if mimetype:
            response.content_type = mimetype
        response.etag = etag

        await response.prepare(request)
        try:
            with path_fichier.open(mode='rb') as input_file:
                if start is not None and start > 0:
                    await asyncio.to_thread(input_file.seek, start, 0)
                    position = start
                else:
                    position = 0

                while True:
                    chunk = await asyncio.to_thread(input_file.read, 64*1024)
                    if not chunk:
                        break

                    try:
                        if end is not None and position + len(chunk) > end:
                            taille_chunk = end - position + 1
                            await response.write(chunk[:taille_chunk])
                            break  # Termine
                        else:
                            await response.write(chunk)
                    except (ConnectionError, ConnectionResetError):
                        pass  # Ok, browser changing position or stopping

                    position += len(chunk)
        except ConnectionResetError:
            self.__logger.debug("Connection reset pour fuuid %s" % fuuid)
        except Exception:
            self.__logger.exception("stream_reponse Erreur stream fichier %s" % fuuid)
        finally:
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

