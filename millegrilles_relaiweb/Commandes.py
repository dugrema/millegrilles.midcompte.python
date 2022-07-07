import logging

from cryptography.x509.extensions import ExtensionNotFound

# from millegrilles_senseurspassifs.EtatSenseursPassifs import EtatSenseursPassifs
from millegrilles_messages.messages import Constantes
from millegrilles_messages.messages.MessagesModule import MessageProducerFormatteur
from millegrilles_messages.messages.MessagesModule import MessageWrapper


class CommandHandler:

    def __init__(self, etat_senseurspassifs):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self._etat_instance = etat_senseurspassifs
        # self._modules_handler = modules_handler
        # self.__routing_keys_modules = modules_handler.get_routing_key_consumers()

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
            # if exchange == Constantes.SECURITE_PUBLIC and Constantes.SECURITE_PUBLIC in exchanges:
            #     if Constantes.ROLE_CORE in roles:
            #         if action == ConstantesInstance.EVENEMENT_TOPOLOGIE_FICHEPUBLIQUE:
            #             return await self.sauvegarder_fiche_publique(message)
            # if routing_key in self.__routing_keys_modules:
            #     return await self._modules_handler.recevoir_confirmation_lecture(message)

            if reponse is None:
                reponse = {'ok': False, 'err': 'Commande inconnue ou acces refuse'}
        except Exception as e:
            self.__logger.exception("Erreur execution commande")
            reponse = {'ok': False, 'err': str(e)}

        return reponse

    # async def transmettre_catalogue(self, producer: MessageProducerFormatteur):
    #     self.__logger.info("Transmettre catalogues")
    #     path_catalogues = self._etat_instance.configuration.path_catalogues
    #
    #     liste_fichiers_apps = listdir(path_catalogues)
    #
    #     info_apps = [path.join(path_catalogues, f) for f in liste_fichiers_apps if f.endswith('.json.xz')]
    #     for app_path in info_apps:
    #         with lzma.open(app_path, 'rt') as fichier:
    #             app_transaction = json.load(fichier)
    #
    #         commande = {"catalogue": app_transaction}
    #         await producer.executer_commande(commande, domaine=Constantes.DOMAINE_CORE_CATALOGUES,
    #                                          action='catalogueApplication', exchange=Constantes.SECURITE_PROTEGE,
    #                                          nowait=True)
    #
    #     return {'ok': True}
