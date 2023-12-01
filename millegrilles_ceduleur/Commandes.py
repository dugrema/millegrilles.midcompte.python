import logging

from cryptography.x509.extensions import ExtensionNotFound

from millegrilles_messages.messages import Constantes
from millegrilles_messages.messages.MessagesModule import MessageProducerFormatteur
from millegrilles_messages.messages.MessagesModule import MessageWrapper
from millegrilles_media import Constantes as ConstantesMedia


class CommandHandler:

    def __init__(self, etat_ceduleur):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self._etat_instance = etat_ceduleur

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
            # if exchange == Constantes.SECURITE_PRIVE and Constantes.SECURITE_PRIVE in exchanges:
            #     if type_message == 'evenement':
            #         if action in [ConstantesMedia.EVENEMENT_JOB_IMAGE_DISPONIBLE, ConstantesMedia.EVENEMENT_JOB_VIDEO_DISPONIBLE]:
            #             await self._intake_images.trigger_traitement()
            #             if self._intake_videos is not None:
            #                 await self._intake_videos.trigger_traitement()
            #             return
            #         elif action == ConstantesMedia.EVENEMENT_ANNULER_JOB_VIDEO:
            #             await self._intake_videos.annuler_job(message.parsed, False)
            if reponse is None:
                reponse = {'ok': False, 'err': 'Commande inconnue ou acces refuse'}

        except Exception as e:
            self.__logger.exception("Erreur execution commande")
            reponse = {'ok': False, 'err': str(e)}

        return reponse
