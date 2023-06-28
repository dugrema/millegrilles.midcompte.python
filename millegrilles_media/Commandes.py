import asyncio
import logging
import requests

from cryptography.x509.extensions import ExtensionNotFound

from millegrilles_messages.messages import Constantes
from millegrilles_messages.messages.MessagesModule import MessageProducerFormatteur
from millegrilles_messages.messages.MessagesModule import MessageWrapper
from millegrilles_media import Constantes as ConstantesMedia


class CommandHandler:

    def __init__(self, etat_senseurspassifs, requetes_handler, intake_images, intake_videos):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self._etat_instance = etat_senseurspassifs
        self._requetes_handler = requetes_handler
        self._intake_images = intake_images
        self._intake_videos = intake_videos

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
            user_id = enveloppe.get_user_id
        except ExtensionNotFound:
            user_id = list()

        try:
            delegation_globale = enveloppe.get_delegation_globale
        except ExtensionNotFound:
            delegation_globale = None

        try:
            # if exchange == Constantes.SECURITE_SECURE and Constantes.SECURITE_SECURE in exchanges:
            #     if type_message == 'evenement':
            #         if action == ConstantesMedia.EVENEMENT_REINDEXER_CONSIGNATION:
            #             return await self._intake_handler.reset_index_fichiers()
            # elif exchange == Constantes.SECURITE_PRIVE and user_id is not None:
            #     if type_message == 'requete' and action in [ConstantesMedia.REQUETE_FICHIERS]:
            #         reponse = await self._requetes_handler.traiter_requete(message)
            if exchange == Constantes.SECURITE_PRIVE and Constantes.SECURITE_PRIVE in exchanges:
                if type_message == 'evenement':
                    if action in [ConstantesMedia.EVENEMENT_CONSIGNATION_PRIMAIRE]:
                        # Declencher tous les intake
                        await self._intake_images.trigger_fichiers()
                        await self._intake_images.trigger_videos()
                        return
            if reponse is None:
                reponse = {'ok': False, 'err': 'Commande inconnue ou acces refuse'}

        except Exception as e:
            self.__logger.exception("Erreur execution commande")
            reponse = {'ok': False, 'err': str(e)}

        return reponse
