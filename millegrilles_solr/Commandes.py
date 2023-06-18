import asyncio
import logging
import requests

from cryptography.x509.extensions import ExtensionNotFound

from millegrilles_messages.messages import Constantes
from millegrilles_messages.messages.MessagesModule import MessageProducerFormatteur
from millegrilles_messages.messages.MessagesModule import MessageWrapper
from millegrilles_solr import Constantes as ConstantesRelaiSolr


class CommandHandler:

    def __init__(self, etat_senseurspassifs, requetes_handler, intake_handler):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self._etat_instance = etat_senseurspassifs
        self._requetes_handler = requetes_handler
        self._intake_handler = intake_handler

    async def executer_commande(self, producer: MessageProducerFormatteur, message: MessageWrapper):
        reponse = None

        routing_key = message.routing_key
        exchange = message.exchange
        if exchange is None or exchange == '':
            self.__logger.warning("Message reponse recu sur Q commande, on le drop (RK: %s)" % routing_key)
            return

        if message.est_valide is False:
            return {'ok': False, 'err': 'Signature ou certificat invalide'}

        action = routing_key.split('.').pop()
        type_message = routing_key.split('.')[0]
        enveloppe = message.certificat

        try:
            exchanges = enveloppe.get_exchanges
        except ExtensionNotFound:
            exchanges = list()

        try:
            roles = enveloppe.get_roles
        except ExtensionNotFound:
            roles = list()

        try:
            delegation_globale = enveloppe.get_delegation_globale
        except ExtensionNotFound:
            delegation_globale = None

        try:
            if exchange == Constantes.SECURITE_PRIVE and Constantes.SECURITE_PRIVE in exchanges:
                if type_message == 'requete' and action in [ConstantesRelaiSolr.REQUETE_FICHIERS]:
                    reponse = await self._requetes_handler.traiter_requete(message)
                elif type_message == 'evenement' and action in [ConstantesRelaiSolr.EVENEMENT_CONSIGNATION_PRIMAIRE]:
                    reponse = await self._intake_handler.trigger_fichiers()
            if reponse is None:
                reponse = {'ok': False, 'err': 'Commande inconnue ou acces refuse'}

        except Exception as e:
            self.__logger.exception("Erreur execution commande")
            reponse = {'ok': False, 'err': str(e)}

        return reponse

    async def relai_web(self, method: str, message: MessageWrapper):
        commande = message.parsed
        self.__logger.debug("Commande relai web : %s", commande)
        try:
            certificat = message.certificat
        except (AttributeError, KeyError):
            return {'ok': False, 'code': 400, 'err': 'Certificat absent'}

        exchanges = certificat.get_exchanges
        if Constantes.SECURITE_PUBLIC in exchanges:
            contenu = commande

            url_requete = contenu['url']
            params = {
                'url': url_requete,
                'timeout': contenu.get('timeout') or 20,
            }

            # Copier parametres optionnels
            params_optionnels = ['headers', 'data', 'json']
            for nom_param in params_optionnels:
                if contenu.get(nom_param) is not None:
                    params[nom_param] = contenu[nom_param]

            flag_erreur_https = False
            if method.lower() == 'get':
                request_method = requests.get
            elif method.lower() == 'post':
                request_method = requests.post
            else:
                return {'ok': False, 'code': 400, 'err': 'Methode inconnue'}

            try:
                response = await asyncio.to_thread(request_method, **params)
            except requests.exceptions.SSLError:
                self.__logger.debug("Erreur certificat https, ajouter un flag certificat invalide")
                flag_erreur_https = True
                params['verify'] = False  # Desactiver verification certificat https
                response = await asyncio.to_thread(request_method, **params)
            except requests.exceptions.ReadTimeout:
                self.__logger.error("Erreur timeout sur %s", params['url'])
                return {'ok': False, 'url': url_requete, 'code': 408, 'err': 'Methode inconnue'}

            self.__logger.debug("Response : %s" % response)

            if 200 <= response.status_code < 300:
                headers = response.headers
                header_dict = {}
                for header_key in headers.keys():
                    header_dict[header_key] = headers.get(header_key)
                try:
                    json_response = response.json()
                    return {
                        'headers': header_dict, 'json': json_response, 'code': response.status_code,
                        'method': method, 'url': url_requete, 'https_verify_ok': not flag_erreur_https}
                except:
                    # Encoder reponse en multibase
                    return {
                        'headers': header_dict, 'text': response.text, 'code': response.status_code,
                        'method': method, 'url': url_requete, 'https_verify_ok': not flag_erreur_https}
            else:
                # Erreur
                return {'ok': False, 'code': response.status_code, 'err': response.text, 'method': method,
                        'url': url_requete}

        else:
            return {'ok': False, 'code': 403, 'err': 'Not authorized'}
