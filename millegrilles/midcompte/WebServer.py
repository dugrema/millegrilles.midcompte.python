import aiohttp
import asyncio
import logging

from aiohttp import web
from aiohttp.web_request import Request
from asyncio import Event
from asyncio.exceptions import TimeoutError
from os import path
from ssl import SSLContext
from typing import Optional

from millegrilles.messages import Constantes
from millegrilles.midcompte.Configuration import ConfigurationWeb
from millegrilles.midcompte.EtatMidcompte import EtatMidcompte
from millegrilles.messages.EnveloppeCertificat import EnveloppeCertificat


class WebServer:

    def __init__(self, etat_midcompte: EtatMidcompte):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__etat_midcompte = etat_midcompte

        self.__app = web.Application()
        self.__stop_event: Optional[Event] = None
        self.__configuration = ConfigurationWeb()
        self.__ssl_context: Optional[SSLContext] = None

        self.__site_web_443 = None

    def setup(self, configuration: Optional[dict] = None):
        self._charger_configuration(configuration)
        self._preparer_routes()
        self._charger_ssl()

    def _charger_configuration(self, configuration: Optional[dict] = None):
        self.__configuration.parse_config(configuration)

    def _preparer_routes(self):
        self.__app.add_routes([
            web.post('/administration/ajouterCompte', self.handle_ajouter_compte),
        ])

    def _charger_ssl(self):
        self.__ssl_context = SSLContext()
        self.__ssl_context.load_cert_chain(self.__configuration.web_cert_pem_path,
                                           self.__configuration.web_key_pem_path)

    async def rediriger_root(self, request):
        return web.HTTPTemporaryRedirect(location='/installation')

    async def handle_ajouter_compte(self, request: Request):
        headers = request.headers
        verified = headers.get('Verified')
        client_cert = headers.get('X-Client-Cert')
        dn = headers.get('DN')
        self.__logger.debug("AJouter compte %s" % dn)

        # Cleanup certificat
        client_cert = client_cert.replace('\t', '')

        enveloppe = EnveloppeCertificat.from_pem(client_cert)

        if verified == 'SUCCESS':
            info = {
                'dn': dn,
                'certificat': enveloppe,
            }

            try:
                await self.__etat_midcompte.ajouter_compte(info)
                return web.HTTPCreated()
            except Exception:
                self.__logger.exception("Erreur ajout compte %s" % dn)
                return web.HTTPInternalServerError()
        else:
            return web.HTTPForbidden()

    async def entretien(self):
        self.__logger.debug('Entretien')

    async def run(self, stop_event: Optional[Event] = None):
        if stop_event is not None:
            self.__stop_event = stop_event
        else:
            self.__stop_event = Event()

        runner = web.AppRunner(self.__app)
        await runner.setup()
        port = self.__configuration.port
        # Configuration pour site sur port 443 (utilise si nginx n'est pas configure)
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


def main():
    logging.basicConfig()
    logging.getLogger(__name__).setLevel(logging.DEBUG)
    logging.getLogger('millegrilles').setLevel(logging.DEBUG)

    server = WebServer()
    server.setup()
    asyncio.run(server.run())


if __name__  == '__main__':
    main()
