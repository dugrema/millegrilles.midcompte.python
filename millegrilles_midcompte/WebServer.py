import asyncio
import logging

from aiohttp import web
from aiohttp.web_request import Request
from asyncio import Event
from asyncio.exceptions import TimeoutError
from ssl import SSLContext
from typing import Optional

from millegrilles_midcompte.Configuration import ConfigurationWeb
from millegrilles_midcompte.EtatMidcompte import EtatMidcompte
from millegrilles_messages.messages.EnveloppeCertificat import EnveloppeCertificat


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
        self.__logger.debug("Charger certificat %s" % self.__configuration.web_cert_pem_path)
        self.__ssl_context.load_cert_chain(self.__configuration.web_cert_pem_path,
                                           self.__configuration.web_key_pem_path)

    async def rediriger_root(self, request):
        return web.HTTPTemporaryRedirect(location='/installation')

    async def handle_ajouter_compte(self, request: Request):
        headers = request.headers
        dn = headers.get('DN')
        self.__logger.debug("AJouter compte %s" % dn)

        verified = headers.get('Verified')
        if verified == 'SUCCESS':
            # Cleanup certificat
            client_cert = headers.get('X-Client-Cert')
            client_cert = client_cert.replace('\t', '')
            enveloppe = EnveloppeCertificat.from_pem(client_cert)

            idmg = enveloppe.idmg
            if idmg != self.__etat_midcompte.idmg:
                self.__logger.debug("ECHEC ajout compte (%s) - le certificat n'est pas pour la bonne millegrille" % dn)
                return web.HTTPForbidden()

            exchanges = enveloppe.get_exchanges
            if exchanges is None or len(exchanges) == 0:
                self.__logger.debug("ECHEC ajout compte (%s) - le certificat n'a pas d'exchanges" % dn)
                return web.HTTPForbidden()

            info = {
                'dn': dn,
                'certificat': enveloppe,
            }

            try:
                await self.__etat_midcompte.ajouter_compte(info)

                if self.__etat_midcompte.configuration.mq_url is None:
                    # HTTP 202 - indique au client qu'il doit aussi se connecter au serveur 3.protege (avec mq)
                    return web.HTTPAccepted()

                # HTTP 201 - Comptes MQ et Mongo crees
                return web.HTTPCreated()

            except Exception:
                self.__logger.exception("Erreur ajout compte %s" % dn)
                return web.HTTPInternalServerError()
        else:
            self.__logger.error("Acces refuse %s" % dn)
            self.__logger.debug("Acces refuse HEADERS : %s" % headers)
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
