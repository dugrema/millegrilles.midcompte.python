import logging

from typing import Callable, Coroutine, Any

from cryptography.x509 import ExtensionNotFound

from millegrilles_messages.bus.BusContext import MilleGrillesBusContext
from millegrilles_messages.messages import Constantes
from millegrilles_messages.bus.PikaChannel import MilleGrillesPikaChannel
from millegrilles_messages.bus.PikaQueue import MilleGrillesPikaQueueConsumer, RoutingKey
from millegrilles_messages.messages.MessagesModule import MessageWrapper
from millegrilles_solr import Constantes as ConstantesRelaiSolr
from millegrilles_solr.Context import SolrContext
from millegrilles_solr.SolrManager import SolrManager


class MgbusHandler:

    def __init__(self, context: SolrContext, solr_manager: SolrManager):
        self.__logger = logging.getLogger(__name__+'.'+self.__class__.__name__)
        self.__context = context
        self.__solr_manager = solr_manager

    async def setup(self):
        channel_exclusive = create_exclusive_q_channel(self.__context, self.on_exclusive_message)
        await self.__context.bus_connector.add_channel(channel_exclusive)
        channel_requests_processing = create_requests_q_channel(self.__context, self.on_request_message)
        await self.__context.bus_connector.add_channel(channel_requests_processing)
        channel_commands = create_commands_q_channel(self.__context, self.on_command_message)
        await self.__context.bus_connector.add_channel(channel_commands)

    async def run(self):
        await self.__context.wait()

    async def on_exclusive_message(self, message: MessageWrapper):

        # Authorization check - 3.protege/CoreTopologie
        enveloppe = message.certificat
        try:
            domaines = enveloppe.get_domaines
        except ExtensionNotFound:
            domaines = list()
        try:
            roles = enveloppe.get_roles
        except ExtensionNotFound:
            roles = list()
        try:
            exchanges = enveloppe.get_exchanges
        except ExtensionNotFound:
            exchanges = list()

        action = message.routage['action']

        if 'CoreTopologie' in domaines and Constantes.SECURITE_PROTEGE in exchanges and action == 'filehostingUpdate':
            # File hosts updated, reload configuration
            await self.__solr_manager.reload_filehost_configuration()
            return None
        elif 'filecontroler' in roles and Constantes.SECURITE_PUBLIC in exchanges and action == ConstantesRelaiSolr.EVENEMENT_NEW_FUUID:
            # File hosts updated, reload configuration
            await self.__solr_manager.trigger_fetch_jobs()
            return None
        elif Constantes.DOMAINE_GROS_FICHIERS in domaines and Constantes.SECURITE_PROTEGE in exchanges:
            if action == ConstantesRelaiSolr.EVENEMENT_FILES_TO_INDEX:
                await self.__solr_manager.trigger_fetch_jobs()
                return None

        self.__logger.info("on_exclusive_message Ignoring unknown action %s" % action)
        return None

    async def on_request_message(self, message: MessageWrapper):

        payload = message.parsed
        action = message.routage['action']

        # Authorization check - user_id
        enveloppe = message.certificat
        try:
            domaines = enveloppe.get_domaines
        except ExtensionNotFound:
            domaines = list()
        try:
            exchanges = enveloppe.get_exchanges
        except ExtensionNotFound:
            exchanges = list()
        if 'GrosFichiers' in domaines and Constantes.SECURITE_PROTEGE in exchanges:
            pass  # GrosFichiers
        else:
            return False  # Ignore message

        try:
            user_id = payload['user_id']
        except ExtensionNotFound:
            return False  # No user_id, ignore message

        if action == ConstantesRelaiSolr.REQUETE_FICHIERS:
            return await self.__solr_manager.query(user_id, payload)

        self.__logger.info("on_request_message Ignoring unknown action %s" % action)
        return False

    async def on_command_message(self, message: MessageWrapper):

        # Authorization check - 3.protege/CoreTopologie
        enveloppe = message.certificat

        message_kind = message.kind
        action = message.routage['action']
        payload = message.parsed

        if message_kind == Constantes.KIND_COMMANDE:
            if action == ConstantesRelaiSolr.COMMANDE_SUPPRIMER_TUUIDS and Constantes.SECURITE_PROTEGE in enveloppe.get_exchanges:
                return await self.__solr_manager.supprimer_tuuids(payload)
            elif action == ConstantesRelaiSolr.COMMANDE_REINDEXER_CONSIGNATION and Constantes.SECURITE_PROTEGE in enveloppe.get_exchanges:
                return await self.__solr_manager.reset_index_fichiers()

        self.__logger.info("on_command_message Ignoring unknown action %s" % action)
        return False


def create_exclusive_q_channel(context: MilleGrillesBusContext, on_message: Callable[[MessageWrapper], Coroutine[Any, Any, None]]) -> MilleGrillesPikaChannel:
    exclusive_q_channel = MilleGrillesPikaChannel(context, prefetch_count=20)
    exclusive_q = MilleGrillesPikaQueueConsumer(context, on_message, None, exclusive=True, arguments={'x-message-ttl': 30_000})
    exclusive_q.add_routing_key(RoutingKey(Constantes.SECURITE_PUBLIC, 'evenement.CoreTopologie.filehostingUpdate'))
    exclusive_q.add_routing_key(RoutingKey(Constantes.SECURITE_PUBLIC, f'evenement.filecontroler.{ConstantesRelaiSolr.EVENEMENT_NEW_FUUID}'))
    exclusive_q.add_routing_key(RoutingKey(Constantes.SECURITE_PROTEGE, f'evenement.GrosFichiers.{ConstantesRelaiSolr.EVENEMENT_FILES_TO_INDEX}'))
    exclusive_q_channel.add_queue(exclusive_q)
    return exclusive_q_channel


def create_requests_q_channel(context: MilleGrillesBusContext, on_message: Callable[[MessageWrapper], Coroutine[Any, Any, bool | dict]]) -> MilleGrillesPikaChannel:
    requests_q_channel = MilleGrillesPikaChannel(context, prefetch_count=3)
    requests_q = MilleGrillesPikaQueueConsumer(context, on_message, "solrrelai/requests", arguments={'x-message-ttl': 30_000})
    requests_q.add_routing_key(RoutingKey(Constantes.SECURITE_PROTEGE, f'requete.solrrelai.{ConstantesRelaiSolr.REQUETE_FICHIERS}'))
    requests_q_channel.add_queue(requests_q)
    return requests_q_channel


def create_commands_q_channel(context: MilleGrillesBusContext, on_message: Callable[[MessageWrapper], Coroutine[Any, Any, None]]) -> MilleGrillesPikaChannel:
    commands_q_channel = MilleGrillesPikaChannel(context, prefetch_count=1)
    commands_q = MilleGrillesPikaQueueConsumer(context, on_message, "solrrelai/volatiles", arguments={'x-message-ttl': 180_000})
    commands_q.add_routing_key(RoutingKey(Constantes.SECURITE_PROTEGE, f'commande.solrrelai.{ConstantesRelaiSolr.COMMANDE_REINDEXER_CONSIGNATION}'))
    commands_q.add_routing_key(RoutingKey(Constantes.SECURITE_PROTEGE, f'commande.solrrelai.{ConstantesRelaiSolr.COMMANDE_SUPPRIMER_TUUIDS}'))
    commands_q_channel.add_queue(commands_q)
    return commands_q_channel
