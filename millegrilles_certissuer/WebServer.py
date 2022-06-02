import asyncio
import datetime
import json
import logging

from aiohttp import web
from asyncio import Event
from asyncio.exceptions import TimeoutError
from typing import Optional

from millegrilles_messages.messages import Constantes
from millegrilles_certissuer.Configuration import ConfigurationWeb
from millegrilles_certissuer.EtatCertissuer import EtatCertissuer
from millegrilles_certissuer.CertificatHandler import CertificatHandler
from millegrilles_messages.certificats.Generes import DUREE_CERT_DEFAUT


class WebServer:

    def __init__(self, etat_certissuer: EtatCertissuer):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__etat_certissuer = etat_certissuer

        self.__configuration = ConfigurationWeb()
        self.__app = web.Application()
        self.__certificat_handler = CertificatHandler(self.__configuration, self.__etat_certissuer)
        self.__stop_event: Optional[Event] = None

    def setup(self, configuration: Optional[dict] = None):
        self._charger_configuration(configuration)
        self._preparer_routes()

    def _charger_configuration(self, configuration: Optional[dict] = None):
        self.__configuration.parse_config(configuration)

    def _preparer_routes(self):
        self.__app.add_routes([
            web.get('/csr', self.handle_csr),
            web.post('/installer', self.handle_installer),
            web.post('/signerModule', self.handle_signer_module),
            web.post('/signerUsager', self.handle_signer_usager_interne),
            web.post('/renouvelerInstance', self.handle_renouveler_instance),
            web.post('/certissuerInterne/signerUsager', self.handle_signer_usager_interne),

            # Commandes relayees par nginx
            web.post('/certissuer/signerUsager', self.handle_signer_usager),
            web.post('/certissuer/signerModule', self.handle_signer_module),
            web.post('/certissuer/renouvelerInstance', self.handle_renouveler_instance),
        ])

    async def handle_csr(self, request):
        csr_str = self.__etat_certissuer.get_csr()
        return web.Response(text=csr_str)

    async def handle_installer(self, request):
        info_cert = await request.json()
        self.__logger.debug("handle_installer params\n%s" % json.dumps(info_cert, indent=2))

        try:
            await self.__etat_certissuer.sauvegarder_certificat(info_cert)
            self.__logger.debug("Sauvegarde du certificat intermediaire OK")
        except:
            self.__logger.exception("Erreur sauvegarde certificat")
            return web.HTTPForbidden()

        # Generer le certificat pour l'application d'instance
        csr_instance = info_cert['csr_instance']
        securite = info_cert['securite']
        cert_instance = self.__certificat_handler.generer_certificat_instance(csr_instance, securite)
        self.__logger.debug("Nouveau certificat d'instance\n%s" % cert_instance)
        return web.json_response({'certificat': cert_instance}, status=201)

    async def handle_renouveler_instance(self, request):
        info_cert = await request.json()
        self.__logger.debug("handle_installer params\n%s" % json.dumps(info_cert, indent=2))

        # Valider signature de request (doit etre role instance, niveau de securite suffisant pour exchanges)
        enveloppe = await self.__etat_certissuer.validateur_messages.verifier(info_cert)

        # Le certificat doit avoir le role instance
        roles_enveloppe = enveloppe.get_roles
        if 'instance' not in roles_enveloppe:
            return web.HTTPForbidden()

        # Les niveaux de securite demandes doivent etre supporte par le certificat demandeur
        securite_enveloppe = enveloppe.get_exchanges
        if Constantes.SECURITE_PROTEGE in securite_enveloppe:
            niveau_securite = Constantes.SECURITE_PROTEGE
        elif Constantes.SECURITE_PROTEGE in securite_enveloppe:
            niveau_securite = Constantes.SECURITE_PRIVE
        elif Constantes.SECURITE_PROTEGE in securite_enveloppe:
            niveau_securite = Constantes.SECURITE_PUBLIC
        else:
            self.__logger.error("handle_renouveler_instance() Niveau de securite non supporte : %s" % securite_enveloppe)
            return web.HTTPBadRequest()

        secondes = self.get_duree_certificat()
        if secondes is not None:
            duree = datetime.timedelta(seconds=secondes)
        else:
            duree = DUREE_CERT_DEFAUT

        # Generer le certificat pour l'application d'instance
        csr_instance = info_cert['csr_instance']
        cert_instance = self.__certificat_handler.generer_certificat_instance(csr_instance, niveau_securite, duree)
        self.__logger.debug("Nouveau certificat d'instance\n%s" % cert_instance)
        return web.json_response({'certificat': cert_instance})

    async def handle_signer_module(self, request):
        info_cert = await request.json()
        self.__logger.debug("handle_installer params\n%s" % json.dumps(info_cert, indent=2))

        # Valider signature de request (doit etre role instance, niveau de securite suffisant pour exchanges)
        enveloppe = await self.__etat_certissuer.validateur_messages.verifier(info_cert)

        # Le certificat doit avoir le role instance ou core
        roles_enveloppe = enveloppe.get_roles
        if 'instance' not in roles_enveloppe and 'core' not in roles_enveloppe:
            return web.HTTPForbidden()

        # Les niveaux de securite demandes doivent etre supporte par le certificat demandeur
        securite_enveloppe = enveloppe.get_exchanges
        try:
            for ex in info_cert['exchanges']:
                if ex == Constantes.SECURITE_SECURE:
                    ex = Constantes.SECURITE_PROTEGE  # Niveau protege permet de creer certificat secure
                if ex not in securite_enveloppe:
                    self.__logger.info('Niveau de securite %s demande par un certificat qui ne le supporte pas' % ex)
                    return web.HTTPForbidden()
        except KeyError:
            pass

        secondes = self.get_duree_certificat()

        if secondes is not None:
            info_cert['duree'] = secondes

        chaine = self.__certificat_handler.generer_certificat_module(info_cert)
        return web.json_response({'ok': True, 'certificat': chaine})

    def get_duree_certificat(self):
        params_temps = dict()

        try:
            duree_jours = int(self.__etat_certissuer.configuration.duree_certificats_jours)
            params_temps['days'] = duree_jours
        except TypeError:
            pass

        try:
            duree_minutes = int(self.__etat_certissuer.configuration.duree_certificats_minutes)
            params_temps['minutes'] = duree_minutes
        except TypeError:
            pass

        if len(params_temps) > 0:
            # Trouver duree en secondes
            secondes = int(datetime.timedelta(**params_temps).seconds)
        else:
            secondes = None

        return secondes

    async def handle_signer_usager_interne(self, request: web.Request):
        """
        Appel direct au module (sans /certissuer)
        :param request:
        :return:
        """
        info_cert = await request.json()
        self.__logger.debug("handle_signer_usager_interne params\n%s" % json.dumps(info_cert, indent=2))

        # Valider signature de request (doit etre role instance, niveau de securite suffisant pour exchanges)
        enveloppe = await self.__etat_certissuer.validateur_messages.verifier(info_cert)

        # Le certificat doit avoir le role instance ou core
        roles_enveloppe = enveloppe.get_roles
        if 'core' not in roles_enveloppe:
            return web.HTTPForbidden()

        chaine = self.__certificat_handler.generer_certificat_usager(info_cert)

        return web.json_response({'ok': True, 'certificat': chaine})

    async def handle_signer_usager(self, request: web.Request):
        """
        Signe un certificat d'usager qui provient d'un serveur externe (relaye via nginx)
        :param request:
        :return:
        """
        return web.HTTPNotImplemented()

    async def entretien(self):
        self.__logger.debug('Entretien')
        try:
            await self.__etat_certissuer.entretien()
        except:
            self.__logger.exception("Erreur entretien etat_certissuer")

    async def run(self, stop_event: Optional[Event] = None):
        if stop_event is not None:
            self.__stop_event = stop_event
        else:
            self.__stop_event = Event()

        runner = web.AppRunner(self.__app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', self.__configuration.port)
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


# def main():
#     logging.basicConfig()
#     logging.getLogger(__name__).setLevel(logging.DEBUG)
#     logging.getLogger('millegrilles').setLevel(logging.DEBUG)
#
#     server = WebServer()
#     server.setup()
#     asyncio.run(server.run())
#
#
# if __name__  == '__main__':
#     main()
