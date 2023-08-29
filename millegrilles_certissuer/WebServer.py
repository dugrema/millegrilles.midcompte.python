import asyncio
import datetime
import json
import logging

from aiohttp import web
from asyncio import Event
from asyncio.exceptions import TimeoutError
from typing import Optional
from cryptography.x509.extensions import ExtensionNotFound

from millegrilles_messages.certificats.Generes import EnveloppeCsr
from millegrilles_messages.messages import Constantes
from millegrilles_certissuer.Configuration import ConfigurationWeb
from millegrilles_certissuer.EtatCertissuer import EtatCertissuer
from millegrilles_certissuer.CertificatHandler import CertificatHandler
from millegrilles_messages.certificats.Generes import DUREE_CERT_DEFAUT


# Roles qui peuvent se connecter directement (https)
ROLES_CONNEXION_PERMIS = ['core', 'instance', 'maitredescles_connexion']

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
            # web.post('/certissuer/signerModule', self.handle_signer_module),
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
        csr_instance = info_cert['csr']
        securite = info_cert['securite']
        duree = self.get_duree_certificat()
        cert_instance = self.__certificat_handler.generer_certificat_instance(csr_instance, securite, duree)
        self.__logger.debug("Nouveau certificat d'instance\n%s" % cert_instance)
        return web.json_response({'certificat': cert_instance}, status=201)

    async def handle_renouveler_instance(self, request):
        info_cert = await request.json()
        self.__logger.debug("handle_renouveler_instance params\n%s" % json.dumps(info_cert, indent=2))

        # Valider signature de request (doit etre role instance, niveau de securite suffisant pour exchanges)
        enveloppe = await self.__etat_certissuer.validateur_messages.verifier(info_cert)

        # Le certificat doit avoir le role instance
        roles_enveloppe = enveloppe.get_roles
        if 'instance' not in roles_enveloppe:
            return web.HTTPForbidden()

        # Les niveaux de securite demandes doivent etre supporte par le certificat demandeur
        securite_enveloppe = enveloppe.get_exchanges
        if Constantes.SECURITE_SECURE in securite_enveloppe:
            niveau_securite = Constantes.SECURITE_SECURE
        elif Constantes.SECURITE_PROTEGE in securite_enveloppe:
            niveau_securite = Constantes.SECURITE_PROTEGE
        elif Constantes.SECURITE_PRIVE in securite_enveloppe:
            niveau_securite = Constantes.SECURITE_PRIVE
        elif Constantes.SECURITE_PUBLIC in securite_enveloppe:
            niveau_securite = Constantes.SECURITE_PUBLIC
        else:
            self.__logger.error("handle_renouveler_instance() Niveau de securite non supporte : %s" % securite_enveloppe)
            return web.HTTPBadRequest()

        duree = self.get_duree_certificat()
        # if secondes is not None:
        #     duree = datetime.timedelta(seconds=secondes)
        # else:
        #     duree = DUREE_CERT_DEFAUT

        contenu = json.loads(info_cert['contenu'])

        # Generer le certificat pour l'application d'instance
        csr_instance = contenu['csr']
        cert_instance = self.__certificat_handler.generer_certificat_instance(csr_instance, niveau_securite, duree)
        self.__logger.debug("Nouveau certificat d'instance\n%s" % cert_instance)
        return web.json_response({'certificat': cert_instance})

    async def handle_signer_module(self, request):
        info_cert = await request.json()
        self.__logger.debug("handle_signer_module params\n%s" % json.dumps(info_cert, indent=2))

        contenu_commande = json.loads(info_cert['contenu'])

        # Valider signature de request (doit etre role instance, niveau de securite suffisant pour exchanges)
        enveloppe = await self.__etat_certissuer.validateur_messages.verifier(info_cert)

        # Le certificat doit avoir le role instance ou core
        try:
            roles_enveloppe = enveloppe.get_roles
        except ExtensionNotFound:
            roles_enveloppe = None
        try:
            delegation_globale = enveloppe.get_delegation_globale
        except ExtensionNotFound:
            delegation_globale = None
        try:
            exchanges = enveloppe.get_exchanges
        except ExtensionNotFound:
            exchanges = None

        if any([r in roles_enveloppe for r in ROLES_CONNEXION_PERMIS]):
            # Les niveaux de securite demandes doivent etre supporte par le certificat demandeur
            securite_enveloppe = enveloppe.get_exchanges
            try:
                for ex in contenu_commande['exchanges']:
                    if ex == Constantes.SECURITE_SECURE:
                        ex = Constantes.SECURITE_PROTEGE  # Niveau protege permet de creer certificat secure
                    if ex not in securite_enveloppe:
                        self.__logger.info(
                            'Niveau de securite %s demande par un certificat qui ne le supporte pas' % ex)
                        return web.HTTPForbidden()
            except (TypeError, KeyError):
                pass

        elif delegation_globale == Constantes.DELEGATION_GLOBALE_PROPRIETAIRE:
            pass
        elif Constantes.SECURITE_SECURE in exchanges:
            pass
        else:
            return web.HTTPForbidden()

        secondes = int(self.get_duree_certificat().total_seconds())

        if secondes is not None:
            contenu_commande['duree'] = secondes

        # Determiner si on renouvelle un certificat d'instance ou signe module
        try:
            roles = contenu_commande['roles']
            if 'instance' in roles and (
                    'instance' in roles_enveloppe or delegation_globale == Constantes.DELEGATION_GLOBALE_PROPRIETAIRE):  # On renouvelle une instance
                # Determiner niveau securite
                csr = contenu_commande['csr']
                enveloppe_csr = EnveloppeCsr.from_str(csr)
                if enveloppe_csr.csr.is_signature_valid is False:
                    return web.json_response({'ok': False, 'err': 'CSR signature mismatch (X.509)'})

                if delegation_globale == Constantes.DELEGATION_GLOBALE_PROPRIETAIRE:
                    niveau = contenu_commande['securite']
                else:
                    # Verifier que le CSR d'instance a le meme instance_id (CN) que l'enveloppe
                    if enveloppe.subject_common_name != enveloppe_csr.cn:
                        return web.json_response({'ok': False, 'err': 'CN mismatch entre certificat et CSR'})

                    niveaux = [Constantes.SECURITE_SECURE, Constantes.SECURITE_PROTEGE, Constantes.SECURITE_PRIVE, Constantes.SECURITE_PUBLIC, None]
                    niveau = None
                    for niveau in niveaux:
                        if niveau in exchanges:
                            break

                if niveau is not None:
                    try:
                        duree_int = contenu_commande['duree']
                        duree = datetime.timedelta(seconds=duree_int)
                    except KeyError:
                        duree = self.get_duree_certificat()
                    chaine = self.__certificat_handler.generer_certificat_instance(csr, niveau, duree)
                else:
                    return web.json_response({'ok': False, 'err': 'Renouvellement instance avec mauvais type de certificat'})
            else:
                chaine = self.__certificat_handler.generer_certificat_module(contenu_commande)
        except KeyError:
            chaine = self.__certificat_handler.generer_certificat_module(contenu_commande)

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
            duree = datetime.timedelta(**params_temps)
        else:
            duree = datetime.timedelta(days=31)

        # secondes = int(duree.total_seconds())

        return duree

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

        # Parse contenu du message
        contenu = json.loads(info_cert['contenu'])

        chaine = self.__certificat_handler.generer_certificat_usager(contenu)

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
