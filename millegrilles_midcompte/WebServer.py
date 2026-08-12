import asyncio
import logging
import urllib.parse

from aiohttp import web
from aiohttp.web_request import Request
from asyncio import Event
from asyncio.exceptions import TimeoutError
from ssl import SSLContext, VerifyMode
from typing import Optional, Union

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
        ssl_context = SSLContext()
        self.__logger.debug("Charger certificat %s" % self.__configuration.web_cert_pem_path)
        private_certfile = self.__configuration.web_cert_pem_path or self.__configuration.web_key_pem_path
        ssl_context.load_cert_chain(private_certfile, self.__configuration.web_key_pem_path)
        ssl_context.load_verify_locations(cafile=self.__configuration.ca_pem_path)
        ssl_context.verify_mode = VerifyMode.CERT_REQUIRED

        self.__ssl_context = ssl_context

    async def rediriger_root(self, request):
        return web.HTTPTemporaryRedirect(location='/installation')

    async def handle_ajouter_compte(self, request: Request):
        client_certificate = get_tls_certificate(request)
        if isinstance(client_certificate, web.Response):
            return client_certificate  # Error response

        headers = request.headers

        if client_certificate is True:  # NGINX trust mode
            dn = headers.get('DN')
            self.__logger.debug(f"Add account for {dn}")

            verified = headers.get('Verified')
            client_cert = headers.get('X-Client-Cert')
            if verified == 'SUCCESS' and client_cert:  # nginx header
                client_cert = urllib.parse.unquote(client_cert)
                enveloppe = EnveloppeCertificat.from_pem(client_cert)
            else:
                return web.HTTPForbidden()
        elif isinstance(client_certificate, EnveloppeCertificat):  # Direct TLS connection to midcompte
            enveloppe = client_certificate
            dn = enveloppe.subject_organizational_unit_name
        else:
            self.__logger.debug("Acces refuse HEADERS : %s" % headers)
            return web.HTTPForbidden()

        idmg = enveloppe.idmg
        if idmg != self.__etat_midcompte.idmg:
            self.__logger.debug("ECHEC ajout compte (%s) - le certificat n'est pas pour la bonne millegrille" % dn)
            return web.HTTPForbidden()

        exchanges = enveloppe.get_exchanges
        if exchanges is None or len(exchanges) == 0:
            self.__logger.debug("ECHEC ajout compte (%s) - le certificat n'a pas d'exchanges (niveau de securite)" % dn)
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
        site = web.TCPSite(runner, None, port, ssl_context=self.__ssl_context)
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


def get_tls_certificate(request: web.Request) -> Union[web.Response, bool, EnveloppeCertificat]:
    transport = request.transport

    if not transport:
        return web.Response(text="No TLS transport active", status=400)

    ssl_object = transport.get_extra_info('ssl_object')
    if not ssl_object:
        return web.Response(text="Connection is not secure (TLS/SLL required)", status=400)

    client_cert = ssl_object.getpeercert()
    if not client_cert:
        return web.Response(text="No client certificate provided", status=401)

    try:
        subject_tuples = client_cert['subject']
        subject_map = {item[0][0]: item[0][1] for item in subject_tuples if item}
        if subject_map['organizationalUnitName'] == 'nginx':
            # This is a valid nginx certificate signed by the MilleGrille's CA, we can trust the headers
            return True
    except KeyError:
        pass

    # Extract the original certificate (it is already validated) and return the Enveloppe
    der_cert = ssl_object.getpeercert(binary_form=True)
    if not der_cert:
        return web.Response(text="Unable to get client certificate (DER format)", status=500)
    return EnveloppeCertificat.from_der(der_cert)
