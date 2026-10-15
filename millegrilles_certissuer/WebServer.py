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


# Roles qui peuvent se connecter directement (https)
ROLES_CONNEXION_PERMIS = ['core', 'pki', 'instance', 'maitredescles_connexion']

class WebServer:

    def __init__(self, etat_certissuer: EtatCertissuer):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__etat_certissuer = etat_certissuer

        self.__configuration = ConfigurationWeb()
        self.__app = web.Application()
        self.__certificat_handler = CertificatHandler(self.__configuration, self.__etat_certissuer)
        self.__stop_event: Optional[Event] = None

    async def setup(self, configuration: Optional[dict] = None):
        self._charger_configuration(configuration)
        self._preparer_routes()

    def _charger_configuration(self, configuration: Optional[dict] = None):
        self.__configuration.parse_config(configuration)

    def _preparer_routes(self):
        self.__app.add_routes([
            web.post('/signerModule', self.handle_signer_module),
            web.post('/signerUsager', self.handle_signer_usager_interne),
            web.post('/renouvelerInstance', self.handle_renouveler_instance),
            web.get('/certificate.pem', self.handler_signing_pem_request),
            web.get('/hosting', self.handler_hosting_request),

            web.post('/certissuerInterne/signerUsager', self.handle_signer_usager_interne),
        ])

    async def handle_renouveler_instance(self, request):
        info_cert = await request.json()
        self.__logger.debug("handle_renouveler_instance params\n%s" % json.dumps(info_cert, indent=2))

        # Valider signature de request (doit etre role instance, niveau de securite suffisant pour exchanges)
        try:
            # enveloppe = await self.__etat_certissuer.validateur_messages.verifier(info_cert)
            enveloppe, domain = await self.__certificat_handler.verify_message(info_cert)
        except:
            # Bad signature / certificate
            return web.HTTPForbidden()

        # Le certificat doit avoir le role instance
        roles_enveloppe = enveloppe.get_roles
        if 'instance' not in roles_enveloppe:
            return web.HTTPForbidden()

        try:
            idmg = enveloppe.idmg
        except:
            # Could not find idmg value
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
            return web.HTTPForbidden()

        duree = self.get_duree_certificat()
        # if secondes is not None:
        #     duree = datetime.timedelta(seconds=secondes)
        # else:
        #     duree = DUREE_CERT_DEFAUT

        contenu = json.loads(info_cert['contenu'])

        # Generer le certificat pour l'application d'instance
        csr_instance = contenu['csr']
        cert_instance = self.__certificat_handler.generer_certificat_instance(domain, enveloppe, csr_instance, niveau_securite, duree)
        self.__logger.debug("Nouveau certificat d'instance\n%s" % cert_instance)
        return web.json_response({'certificat': cert_instance})

    async def handle_signer_module(self, request):
        info_cert = await request.json()
        # self.__logger.debug("handle_signer_module params\n%s" % json.dumps(info_cert, indent=2))

        contenu_commande = json.loads(info_cert['contenu'])

        # Valider signature de request (doit etre role instance, niveau de securite suffisant pour exchanges)
        try:
            # enveloppe = await self.__etat_certissuer.validateur_messages.verifier(info_cert)
            enveloppe, domain = await self.__certificat_handler.verify_message(info_cert)
        except:
            # Bad signature/certificate
            return web.HTTPForbidden()

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

        try:
            idmg = enveloppe.idmg
        except:
            self.__logger.debug("Certificate IDMG not available")
            return web.HTTPForbidden()

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

        # secondes = int(self.get_duree_certificat().total_seconds())
        # if secondes is not None:
        #     contenu_commande['duree'] = secondes

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
                    duree = self.get_duree_certificat(niveau=niveau)
                    chaine = self.__certificat_handler.generer_certificat_instance(domain, enveloppe, csr, niveau, duree)
                else:
                    return web.json_response({'ok': False, 'err': 'Renouvellement instance avec mauvais type de certificat'})
            else:
                # Renewing a module using the parameters in the command
                requested_security_levels = contenu_commande['exchanges']
                levels = [Constantes.SECURITE_SECURE, Constantes.SECURITE_PROTEGE, Constantes.SECURITE_PRIVE, Constantes.SECURITE_PUBLIC, None]
                top_level = None
                for top_level in levels:
                    if top_level in requested_security_levels:
                        break
                secondes = int(self.get_duree_certificat(top_level).total_seconds())
                if secondes is not None:
                    contenu_commande['duree'] = secondes
                chaine = self.__certificat_handler.generer_certificat_module(domain, contenu_commande)
        except KeyError:
            secondes = int(self.get_duree_certificat().total_seconds())
            if secondes is not None:
                contenu_commande['duree'] = secondes
            chaine = self.__certificat_handler.generer_certificat_module(domain, contenu_commande)

        return web.json_response({'ok': True, 'certificat': chaine})

    def get_duree_certificat(self, niveau: Optional[str] = None) -> datetime.timedelta:
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
        elif niveau in ['1.public', '2.prive']:
            duree = datetime.timedelta(days=90)
        elif niveau == '3.protege':
            duree = datetime.timedelta(days=31)
        elif niveau == '4.secure':
            duree = datetime.timedelta(days=9)
        else:
            duree = datetime.timedelta(days=180)

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
        try:
            # enveloppe = await self.__etat_certissuer.validateur_messages.verifier(info_cert)
            enveloppe, domain = await self.__certificat_handler.verify_message(info_cert)
        except:
            # Bad certificate / message
            return web.HTTPForbidden()

        # Le certificat doit avoir le role instance ou core
        roles_enveloppe = enveloppe.get_roles
        if 'core' not in roles_enveloppe:
            return web.HTTPForbidden()

        # Parse contenu du message
        contenu = json.loads(info_cert['contenu'])

        chaine = self.__certificat_handler.generer_certificat_usager(domain, contenu)

        return web.json_response({'ok': True, 'certificat': chaine})

    async def handler_signing_pem_request(self, request: web.Request):
        """
        Returns some information about the Signing CA certificate
        :param _request:
        :return:
        """
        idmg = request.query.get('idmg')
        if not idmg:
            return web.HTTPBadRequest()
        domain = await self.__certificat_handler.get_hosting(idmg)

        cert = domain.signing_key.enveloppe
        pem = cert.certificat_pem

        return web.Response(content_type="application/x-pem-file", text=pem)

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
        site = web.TCPSite(runner, None, self.__configuration.port)
        try:
            await site.start()
            self.__logger.info(f"Site demarre sur port {self.__configuration.port}")

            while not self.__stop_event.is_set():
                await self.entretien()
                try:
                    await asyncio.wait_for(self.__stop_event.wait(), 30)
                except TimeoutError:
                    pass
        finally:
            self.__logger.info("Site arrete")
            await runner.cleanup()

    async def handler_hosting_request(self, _request: web.Request):
        """ Return list of hosted systems """
        idmg_list = self.__certificat_handler.list_hosting()
        idmg_list.sort()
        content = '\n'.join(idmg_list) +'\n'
        return web.HTTPOk(text=content)
