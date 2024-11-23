import asyncio
import logging
import datetime

from asyncio import TaskGroup
from typing import Optional, Callable, Coroutine, Any

from cryptography.x509 import ExtensionNotFound

from millegrilles_messages.bus.BusContext import MilleGrillesBusContext
from millegrilles_messages.messages import Constantes
from millegrilles_messages.bus.PikaChannel import MilleGrillesPikaChannel
from millegrilles_messages.bus.PikaQueue import MilleGrillesPikaQueueConsumer, RoutingKey
from millegrilles_messages.messages.MessagesModule import MessageWrapper
from millegrilles_messages.structs.Filehost import Filehost
from millegrilles_solr import Constantes as ConstantesRelaiSolr
from millegrilles_solr.Context import SolrContext
from millegrilles_solr.SolrManager import SolrManager


class MgbusHandler:

    def __init__(self, context: SolrContext, solr_manager: SolrManager):
        self.__logger = logging.getLogger(__name__+'.'+self.__class__.__name__)
        self.__context = context
        self.__solr_manager = solr_manager
        self.__current_filehost_id: Optional[str] = None
        self.__channel_index_processing: Optional[MilleGrillesPikaChannel] = None
        self.__current_index_processing_consumer: Optional[MilleGrillesPikaQueueConsumer] = None
        self.__current_all_index_processing_consumer: Optional[MilleGrillesPikaQueueConsumer] = None
        self.__filehost_update_queue = asyncio.Queue(maxsize=2)

    async def setup(self):
        channel_exclusive = create_exclusive_q_channel(self.__context, self.on_exclusive_message)
        await self.__context.bus_connector.add_channel(channel_exclusive)
        channel_requests_processing = create_requests_q_channel(self.__context, self.on_request_message)
        await self.__context.bus_connector.add_channel(channel_requests_processing)
        channel_commands = create_commands_q_channel(self.__context, self.on_command_message)
        await self.__context.bus_connector.add_channel(channel_commands)

        self.__channel_index_processing = MilleGrillesPikaChannel(self.__context, prefetch_count=1)
        await self.__context.bus_connector.add_channel(self.__channel_index_processing)

    async def run(self):
        async with TaskGroup() as group:
            group.create_task(self.__update_filehosting_task())

        await self.__filehost_update_queue.put(None)  # Exit thread

    async def __update_filehosting_task(self):
        while self.__context.stopping is False:
            filehost = await self.__filehost_update_queue.get()
            if filehost is None:
                break  # Exit condition

            filehost_id = filehost.filehost_id
            if self.__current_filehost_id != filehost_id:
                await self.__activate_processing_listeners(filehost_id)

    async def on_filehosting_update(self, filehost: Optional[Filehost]):
        """
        Callback from context to allow reloading queues/consumers when the filehost configuration changes
        :return:
        """
        self.__logger.debug("on_filehosting_update Triggered")
        await self.__filehost_update_queue.put(filehost)

    async def on_exclusive_message(self, message: MessageWrapper):

        # Authorization check - 3.protege/CoreTopologie
        enveloppe = message.certificat
        try:
            domaines = enveloppe.get_domaines
        except ExtensionNotFound:
            domaines = list()
        try:
            exchanges = enveloppe.get_exchanges
        except ExtensionNotFound:
            exchanges = list()

        if 'CoreTopologie' in domaines and Constantes.SECURITE_PROTEGE in exchanges:
            pass  # CoreTopologie
        else:
            return  # Ignore message

        action = message.routage['action']

        if action == 'filehostingUpdate':
            # File hosts updated, reload configuration
            return await self.__solr_manager.reload_filehost_configuration()

        self.__logger.info("on_exclusive_message Ignoring unknown action %s" % action)

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
            return  # Ignore message

        try:
            user_id = payload['user_id']
        except ExtensionNotFound:
            return  # No user_id, ignore message

        if action == ConstantesRelaiSolr.REQUETE_FICHIERS:
            return await self.__solr_manager.query(user_id, payload)

        self.__logger.info("on_request_message Ignoring unknown action %s" % action)

    async def on_command_message(self, message: MessageWrapper):

        # Authorization check - 3.protege/CoreTopologie
        enveloppe = message.certificat
        try:
            delegation_globale = enveloppe.get_delegation_globale == Constantes.DELEGATION_GLOBALE_PROPRIETAIRE
        except ExtensionNotFound:
            delegation_globale = False
        try:
            user_id = enveloppe.get_user_id
        except ExtensionNotFound:
            user_id = None

        message_kind = message.kind
        action = message.routage['action']
        payload = message.parsed

        if message_kind == Constantes.KIND_COMMANDE:
            if action == ConstantesRelaiSolr.COMMANDE_SUPPRIMER_TUUIDS and user_id:
                return await self.__solr_manager.supprimer_tuuids(user_id, payload)
            elif action == ConstantesRelaiSolr.COMMANDE_REINDEXER_CONSIGNATION and Constantes.SECURITE_PROTEGE in enveloppe.get_exchanges:
                return await self.__reset_index_fichiers()

        self.__logger.info("on_command_message Ignoring unknown action %s" % action)

    async def __activate_processing_listeners(self, filehost_id: str):
        await self.__remove_processing_listeners()
        self.__current_filehost_id = filehost_id

        q_name = f'solrrelai/index'
        self.__logger.debug("Activating processing listener on %s" % q_name)
        index_consumer = MilleGrillesPikaQueueConsumer(
            self.__context, self.on_index_processing_message, q_name,
            auto_delete=True,  # remove queue to avoid piling up messages for a filehost with no processors
            arguments={'x-message-ttl': 180000})
        index_consumer.add_routing_key(
            RoutingKey(Constantes.SECURITE_PROTEGE, f'commande.solrrelai.processIndex'))
        self.__current_all_index_processing_consumer = index_consumer
        await self.__channel_index_processing.add_queue_consume(self.__current_all_index_processing_consumer)

        q_name = f'solrrelai/{filehost_id}/index'
        self.__logger.debug("Activating processing listener on %s" % q_name)
        index_consumer = MilleGrillesPikaQueueConsumer(
            self.__context, self.on_index_processing_message, q_name,
            auto_delete=True,  # remove queue to avoid piling up messages for a filehost with no processors
            arguments={'x-message-ttl': 180000})
        index_consumer.add_routing_key(
            RoutingKey(Constantes.SECURITE_PROTEGE, f'commande.solrrelai.{filehost_id}.processIndex'))
        self.__current_index_processing_consumer = index_consumer
        await self.__channel_index_processing.add_queue_consume(self.__current_index_processing_consumer)

    async def __remove_processing_listeners(self):
        all_index_job_consumer = self.__current_all_index_processing_consumer
        if all_index_job_consumer:
            self.__logger.debug("Removing processing listener on indexing (all)")
            self.__current_all_index_processing_consumer = None
            await self.__channel_index_processing.remove_queue(all_index_job_consumer)

        index_job_consumer = self.__current_index_processing_consumer
        if index_job_consumer:
            self.__logger.debug("Removing processing listener on indexing (filehost)")
            self.__current_index_processing_consumer = None
            await self.__channel_index_processing.remove_queue(index_job_consumer)

    async def on_index_processing_message(self, message: MessageWrapper):
        # Authorization check - 3.protege/CoreTopologie
        enveloppe = message.certificat
        try:
            domaines = enveloppe.get_domaines
        except ExtensionNotFound:
            domaines = list()
        try:
            exchanges = enveloppe.get_exchanges
        except ExtensionNotFound:
            exchanges = list()

        action = message.routage['action']
        estampille = message.estampille
        message_age = datetime.datetime.now().timestamp() - estampille
        payload = message.parsed

        if action == 'processIndex':
            if Constantes.SECURITE_PROTEGE in exchanges and Constantes.DOMAINE_GROS_FICHIERS in domaines and message_age < 180:
                return await self.__solr_manager.process_job(payload)

        self.__logger.info("on_index_processing_message Ignoring unknown index action / wrong security %s" % action)

    async def __reset_index_fichiers(self):
        # Desactiver processing index
        await self.__remove_processing_listeners()
        try:
            await self.__solr_manager.reset_index_fichiers()
            return {'ok': True}
        except Exception as e:
            self.__logger.exception("__reset_index_fichiers Unhandled exception")
            return {'ok': False, 'err': str(e)}
        finally:
            # Reactiver processing index
            await self.__activate_processing_listeners(self.__current_filehost_id)


def create_exclusive_q_channel(context: MilleGrillesBusContext, on_message: Callable[[MessageWrapper], Coroutine[Any, Any, None]]) -> MilleGrillesPikaChannel:
    exclusive_q_channel = MilleGrillesPikaChannel(context, prefetch_count=20)
    exclusive_q = MilleGrillesPikaQueueConsumer(context, on_message, None, exclusive=True, arguments={'x-message-ttl': 300000})
    exclusive_q.add_routing_key(RoutingKey(Constantes.SECURITE_PUBLIC, 'evenement.CoreTopologie.filehostingUpdate'))
    exclusive_q_channel.add_queue(exclusive_q)
    return exclusive_q_channel


def create_requests_q_channel(context: MilleGrillesBusContext, on_message: Callable[[MessageWrapper], Coroutine[Any, Any, None]]) -> MilleGrillesPikaChannel:
    requests_q_channel = MilleGrillesPikaChannel(context, prefetch_count=1)
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
