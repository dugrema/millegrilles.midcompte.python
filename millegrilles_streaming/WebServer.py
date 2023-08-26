import asyncio
import logging
import urllib.parse
import jwt

from aiohttp import web
from aiohttp.web_request import Request
from asyncio import Event
from asyncio.exceptions import TimeoutError
from ssl import SSLContext
from typing import Optional

from millegrilles_streaming.Configuration import ConfigurationWeb
from millegrilles_streaming.EtatStreaming import EtatStreaming
from millegrilles_streaming.Commandes import CommandHandler
from millegrilles_messages.messages.EnveloppeCertificat import EnveloppeCertificat


class WebServer:

    def __init__(self, etat: EtatStreaming, commandes: CommandHandler):
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

    def _preparer_routes(self):
        self.__app.add_routes([
            web.get('/stream_transfert/{fuuid}', self.handle_path_fuuid),
        ])

    def _charger_ssl(self):
        self.__ssl_context = SSLContext()
        self.__logger.debug("Charger certificat %s" % self.__configuration.web_cert_pem_path)
        self.__ssl_context.load_cert_chain(self.__configuration.web_cert_pem_path,
                                           self.__configuration.web_key_pem_path)

    async def handle_path_fuuid(self, request: Request):
        fuuid = request.match_info['fuuid']
        headers = request.headers
        jwt = request.query.get('jwt')

        if jwt is None:
            # Token JWT absent
            self.__logger.debug("handle_path_fuuid ERROR jwt absent pour requete sur fuuid %s" % fuuid)
            return web.HTTPForbidden()

        self.__logger.debug("handle_path_fuuid Requete sur fuuid %s" % fuuid)
        try:
            if await self.verifier_token_jwt(jwt, fuuid) is False:
                self.__logger.debug("handle_path_fuuid ERROR jwt refuse pour requete sur fuuid %s" % fuuid)
                return web.HTTPUnauthorized()

            # HTTP 202 - indique au client qu'il doit aussi se connecter au serveur 3.protege (avec mq)
            # return web.HTTPAccepted()
            # HTTP 200
            #return web.HTTPOk()
            return web.HTTPInternalServerError()  # Fix me

        except Exception:
            self.__logger.exception("handle_path_fuuid ERROR")
            return web.HTTPInternalServerError()

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

    async def verifier_token_jwt(self, token: str, fuuid: str) -> bool:
        # Recuperer kid, charger certificat pour validation
        header = jwt.get_unverified_header(token)
        fingerprint = header['kid']
        enveloppe = await self.__etat.charger_certificat(fingerprint)

        roles = enveloppe.get_roles
        if 'collections' in roles:  # Note - corriger, les JWT devraient etre generes par un domaine
            pass  # Ok
        else:
            # Certificat n'est pas autorise a signer des streams
            return False

        public_key = enveloppe.get_public_key()

        try:
            claims = jwt.decode(token, public_key, algorithms=['EdDSA'])
        except jwt.exceptions.InvalidSignatureError:
            # Signature invalide
            return False

        self.__logger.debug("JWT claims pour %s = %s" % (fuuid, claims))

        return True
