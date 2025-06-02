import datetime
import logging
import asyncio

from asyncio import TaskGroup

from typing import Any, Callable, Coroutine, Optional

from cryptography.x509 import ExtensionNotFound

from millegrilles_media.Configuration import ConfigurationMedia
from millegrilles_media.MediaManager import MediaManager
from millegrilles_messages.messages.MessagesModule import MessageWrapper
from millegrilles_messages.messages import Constantes
from millegrilles_messages.bus.BusContext import MilleGrillesBusContext
from millegrilles_messages.bus.PikaChannel import MilleGrillesPikaChannel
from millegrilles_messages.bus.PikaQueue import MilleGrillesPikaQueueConsumer, RoutingKey
from millegrilles_media.Context import MediaContext
from millegrilles_messages.structs.Filehost import Filehost


class CommandHandler:

    def __init__(self, context: MediaContext, media_manager: MediaManager):
        self.__logger = logging.getLogger(__name__+'.'+self.__class__.__name__)
        self.__context = context
        self.__media_manager = media_manager
        self.__current_filehost_id: Optional[str] = None
        # self.__channel_image_processing: Optional[MilleGrillesPikaChannel] = None
        # self.__current_image_processing_consumer: Optional[MilleGrillesPikaQueueConsumer] = None
        self.__channel_video_processing: Optional[MilleGrillesPikaChannel] = None
        self.__current_video_processing_consumer: Optional[MilleGrillesPikaQueueConsumer] = None
        self.__filehost_update_queue = asyncio.Queue(maxsize=2)

    async def setup(self):
        channel_triggers = create_trigger_q_channel(self.__context, self.on_trigger)
        await self.__context.bus_connector.add_channel(channel_triggers)
        channel_exclusive = create_exclusive_q_channel(self.__context, self.on_exclusive_message)
        await self.__context.bus_connector.add_channel(channel_exclusive)

        configuration: ConfigurationMedia = self.__context.configuration
        # if configuration.image_processing:
        #     self.__channel_image_processing = MilleGrillesPikaChannel(self.__context, prefetch_count=1)
        #     await self.__context.bus_connector.add_channel(self.__channel_image_processing)
        if configuration.video_processing:
            self.__channel_video_processing = MilleGrillesPikaChannel(self.__context, prefetch_count=1)
            await self.__context.bus_connector.add_channel(self.__channel_video_processing)

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

    async def on_trigger(self, message: MessageWrapper):
        enveloppe = message.certificat
        try:
            roles = set(enveloppe.get_roles)
        except ExtensionNotFound:
            roles = list()
        try:
            exchanges = set(enveloppe.get_exchanges)
        except ExtensionNotFound:
            exchanges = list()

        action = message.routage['action']

        if 'filecontroler' in roles and Constantes.SECURITE_PUBLIC in exchanges and action == 'filehostNewFuuid':
            await self.__media_manager.newfile_event_received(message)

    async def on_exclusive_message(self, message: MessageWrapper):

        # Authorization check - 3.protege/CoreTopologie
        enveloppe = message.certificat
        try:
            domaines = set(enveloppe.get_domaines)
        except ExtensionNotFound:
            domaines = list()
        try:
            exchanges = set(enveloppe.get_exchanges)
        except ExtensionNotFound:
            exchanges = list()

        if {'CoreTopologie', 'GrosFichiers'}.isdisjoint(domaines) is False and Constantes.SECURITE_PROTEGE in exchanges:
            pass  # CoreTopologie
        else:
            return None  # Ignore message

        domain = message.routage['domaine']
        action = message.routage['action']

        if domain == 'CoreTopologie' and action == 'filehostingUpdate':
            # File hosts updated, reload configuration
            return await self.__media_manager.reload_filehost_configuration()
        elif domain == 'GrosFichiers' and action == 'jobSupprimee':
            # File hosts updated, reload configuration
            return await self.__media_manager.cancel_job(message, enveloppe)

        self.__logger.info("on_exclusive_message Ignoring unknown action %s" % action)
        return None

    # async def on_image_processing_message(self, message: MessageWrapper):
    #     # Authorization check - 3.protege/CoreTopologie
    #     enveloppe = message.certificat
    #     try:
    #         domaines = enveloppe.get_domaines
    #     except ExtensionNotFound:
    #         domaines = list()
    #     try:
    #         exchanges = enveloppe.get_exchanges
    #     except ExtensionNotFound:
    #         exchanges = list()
    #     try:
    #         delegation_globale = enveloppe.get_delegation_globale
    #     except ExtensionNotFound:
    #         delegation_globale = None
    #
    #     action = message.routage['action']
    #     estampille = message.estampille
    #     message_age = datetime.datetime.now().timestamp() - estampille
    #     payload = message.parsed
    #
    #     if action == 'processImage':
    #         if Constantes.SECURITE_PROTEGE in exchanges and Constantes.DOMAINE_GROS_FICHIERS in domaines and message_age < 180:
    #             return await self.__media_manager.process_image_job(payload)
    #
    #     self.__logger.info("on_volatile_message Ignoring unknown image action / wrong security %s" % action)

    async def on_video_processing_message(self, message: MessageWrapper):
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
        try:
            delegation_globale = enveloppe.get_delegation_globale
        except ExtensionNotFound:
            delegation_globale = None

        action = message.routage['action']
        estampille = message.estampille
        message_age = datetime.datetime.now().timestamp() - estampille
        payload = message.parsed

        if action == 'processVideo':
            if Constantes.SECURITE_PROTEGE in exchanges and Constantes.DOMAINE_GROS_FICHIERS in domaines and message_age < 600:
                return await self.__media_manager.process_video_job(payload)

        self.__logger.info("on_volatile_message Ignoring unknown video action %s" % action)

    async def on_filehosting_update(self, filehost: Optional[Filehost]):
        """
        Callback from context to allow reloading queues/consumers when the filehost configuration changes
        :return:
        """
        self.__logger.debug("on_filehosting_update Triggered")
        await self.__filehost_update_queue.put(filehost)

    async def __activate_processing_listeners(self, filehost_id: str):
        await self.__remove_processing_listeners()
        self.__current_filehost_id = filehost_id

        # if self.__channel_image_processing:
        #     q_name = f'media/{filehost_id}/image'
        #     self.__logger.debug("Activating processing listener on %s" % q_name)
        #     image_consumer = MilleGrillesPikaQueueConsumer(
        #         self.__context, self.on_image_processing_message, q_name,
        #         auto_delete=True,  # remove queue to avoid piling up messages for a filehost with no processors
        #         arguments={'x-message-ttl': 180000})
        #     image_consumer.add_routing_key(
        #         RoutingKey(Constantes.SECURITE_PROTEGE, f'commande.media.{filehost_id}.processImage'))
        #     self.__current_image_processing_consumer = image_consumer
        #     await self.__channel_image_processing.add_queue_consume(self.__current_image_processing_consumer)

        if self.__channel_video_processing:
            q_name = f'media/{filehost_id}/video'
            self.__logger.debug("Activating processing listener on %s" % q_name)
            video_consumer = MilleGrillesPikaQueueConsumer(
                self.__context, self.on_video_processing_message, q_name,
                auto_delete=True,  # remove queue to avoid piling up messages for a filehost with no processors
                arguments={'x-message-ttl': 600000})
            video_consumer.add_routing_key(
                RoutingKey(Constantes.SECURITE_PROTEGE, f'commande.media.{filehost_id}.processVideo'))
            self.__current_video_processing_consumer = video_consumer
            await self.__channel_video_processing.add_queue_consume(self.__current_video_processing_consumer)

    async def __remove_processing_listeners(self):
        # image_consumer = self.__current_image_processing_consumer
        # if image_consumer:
        #     self.__logger.debug("Removing processing listener on images")
        #     self.__current_image_processing_consumer = None
        #     await self.__channel_image_processing.remove_queue(image_consumer)

        video_consumer = self.__current_video_processing_consumer
        if video_consumer:
            self.__logger.debug("Removing processing listener on videos")
            self.__current_video_processing_consumer = None
            await self.__channel_video_processing.remove_queue(video_consumer)


def create_trigger_q_channel(context: MilleGrillesBusContext, on_message: Callable[[MessageWrapper], Coroutine[Any, Any, None]]) -> MilleGrillesPikaChannel:
    # System triggers
    trigger_q_channel = MilleGrillesPikaChannel(context, prefetch_count=1)
    trigger_q = MilleGrillesPikaQueueConsumer(context, on_message, 'media/triggers', arguments={'x-message-ttl': 90000})
    trigger_q.add_routing_key(RoutingKey(Constantes.SECURITE_PUBLIC, f'evenement.ceduleur.{Constantes.EVENEMENT_PING_CEDULE}'))
    trigger_q.add_routing_key(RoutingKey(Constantes.SECURITE_PUBLIC, 'evenement.filecontroler.filehostNewFuuid'))

    trigger_q_channel.add_queue(trigger_q)

    return trigger_q_channel

def create_exclusive_q_channel(context: MilleGrillesBusContext, on_message: Callable[[MessageWrapper], Coroutine[Any, Any, None]]) -> MilleGrillesPikaChannel:
    volatile_q_channel = MilleGrillesPikaChannel(context, prefetch_count=20)
    volatile_q = MilleGrillesPikaQueueConsumer(context, on_message, None, exclusive=True, arguments={'x-message-ttl': 300000})
    volatile_q.add_routing_key(RoutingKey(Constantes.SECURITE_PUBLIC, 'evenement.CoreTopologie.filehostingUpdate'))
    volatile_q.add_routing_key(RoutingKey(Constantes.SECURITE_PRIVE, 'evenement.GrosFichiers.*.jobSupprimee'))

    volatile_q_channel.add_queue(volatile_q)

    return volatile_q_channel
