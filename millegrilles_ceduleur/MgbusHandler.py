import logging

from asyncio import TaskGroup
from typing import Optional

from millegrilles_ceduleur.TickerManager import TickerManager
from millegrilles_messages.bus.BusContext import ForceTerminateExecution


class MgbusHandler:
    """
    MQ access module
    """

    async def run(self):
        self.__logger.debug("MgbusHandler thread started")
        try:
            # await self.__register()

            async with TaskGroup() as group:
                self.__task_group = group
                group.create_task(self.__stop_thread())
                group.create_task(self.__manager.context.bus_connector.run())

        except *Exception:  # Stop on any thread exception
            if self.__manager.context.stopping is False:
                self.__logger.exception("MgbusHandler Unhandled error, closing")
                self.__manager.context.stop()
                raise ForceTerminateExecution()
        self.__task_group = None
        self.__logger.debug("MgbusHandler thread done")

    def __init__(self, manager: TickerManager):
        super().__init__()
        self.__logger = logging.getLogger(__name__+'.'+self.__class__.__name__)
        self.__manager = manager
        self.__task_group: Optional[TaskGroup] = None

    async def __stop_thread(self):
        await self.__manager.context.wait()

    # async def __register(self):
    #     self.__logger.info("Register with the MQ Bus")
    #
    #     context = self.__manager.context
    #
    #     channel_volatile = create_volatile_q_channel(context, self.__on_volatile_message)
    #     await self.__manager.context.bus_connector.add_channel(channel_volatile)
    #
    # async def __on_volatile_message(self, message: MessageWrapper):
    #     # Authorization check
    #     enveloppe = message.certificat
    #     try:
    #         roles = enveloppe.get_roles
    #     except ExtensionNotFound:
    #         roles = list()
    #
    #     if Constantes.ROLE_USAGER not in roles:
    #         return {'ok': False, 'code': 403, 'err': 'Acces denied'}
    #
    #     action = message.routage['action']
    #
    #     if action in ['generate', 'chat']:
    #         return await self.__manager.handle_volalile_query(message)
    #     elif action in ['pull', 'ping', 'getModels']:
    #         return await self.__manager.handle_volalile_request(message)
    #
    #     self.__logger.info("__on_volatile_message Ignoring unknown action %s", message.routing_key)


# def create_volatile_q_channel(context: MilleGrillesBusContext,
#                                on_message: Callable[[MessageWrapper], Coroutine[Any, Any, None]]) -> MilleGrillesPikaChannel:
#
#     q_channel = MilleGrillesPikaChannel(context, prefetch_count=1)
#     q_instance = MilleGrillesPikaQueueConsumer(
#         context, on_message, 'ollama_relai/volatile', arguments={'x-message-ttl': 30_000})
#
#     q_instance.add_routing_key(RoutingKey(
#         Constantes.SECURITE_PROTEGE, f'commande.{OllamaConstants.DOMAIN_OLLAMA_RELAI}.pull'))
#     q_instance.add_routing_key(RoutingKey(
#         Constantes.SECURITE_PRIVE, f'commande.{OllamaConstants.DOMAIN_OLLAMA_RELAI}.generate'))
#     q_instance.add_routing_key(RoutingKey(
#         Constantes.SECURITE_PRIVE, f'commande.{OllamaConstants.DOMAIN_OLLAMA_RELAI}.chat'))
#     q_instance.add_routing_key(RoutingKey(
#         Constantes.SECURITE_PRIVE, f'requete.{OllamaConstants.DOMAIN_OLLAMA_RELAI}.ping'))
#     q_instance.add_routing_key(RoutingKey(
#         Constantes.SECURITE_PRIVE, f'requete.{OllamaConstants.DOMAIN_OLLAMA_RELAI}.getModels'))
#
#     q_channel.add_queue(q_instance)
#     return q_channel
