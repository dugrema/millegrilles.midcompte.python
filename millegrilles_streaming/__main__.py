import asyncio
import logging
from asyncio import TaskGroup
from concurrent.futures.thread import ThreadPoolExecutor

from typing import Awaitable

from millegrilles_streaming.Configuration import StreamingConfiguration
from millegrilles_streaming.Context import StreamingContext
from millegrilles_messages.bus.BusContext import ForceTerminateExecution, StopListener
from millegrilles_messages.bus.PikaConnector import MilleGrillesPikaConnector
from millegrilles_streaming.Intake import IntakeHandler
from millegrilles_streaming.MgbusHandler import MgbusHandler
from millegrilles_streaming.StagingMaintenance import StagingMaintenanceHandler
from millegrilles_streaming.StreamingManager import StreamingManager
from millegrilles_streaming.WebServer import WebServer

LOGGER = logging.getLogger(__name__)


async def force_terminate_task_group():
    """Used to force termination of a task group."""
    raise ForceTerminateExecution()


async def main():
    config = StreamingConfiguration.load()
    context = StreamingContext(config)

    LOGGER.setLevel(logging.INFO)
    LOGGER.info("Starting")

    # Wire classes together, gets awaitables to run
    coros = await wiring(context)

    try:
        # Use taskgroup to run all threads
        async with TaskGroup() as group:
            for coro in coros:
                group.create_task(coro)

            # Create a listener that fires a task to cancel all other tasks
            async def stop_group():
                group.create_task(force_terminate_task_group())
            stop_listener = StopListener(stop_group)
            context.register_stop_listener(stop_listener)

    except* (ForceTerminateExecution, asyncio.CancelledError):
        pass  # Result of the termination task


async def wiring(context: StreamingContext) -> list[Awaitable]:
    # Some threads get used to handle sync events for the duration of the execution. Ensure there are enough.
    loop = asyncio.get_event_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=10))

    # Create instances
    bus_connector = MilleGrillesPikaConnector(context)
    intake = IntakeHandler(context)
    maintenance_handler = StagingMaintenanceHandler(context)
    manager = StreamingManager(context, intake)

    # Access modules
    web_server = WebServer(manager)
    bus_handler = MgbusHandler(manager)

    # Setup, injecting additional dependencies
    context.bus_connector = bus_connector
    # context.add_reload_listener(intake.clear_session)
    await manager.setup()  # Create folders before rest of setup
    await maintenance_handler.setup()
    await web_server.setup()

    # Create tasks
    coros = [
        context.run(),
        maintenance_handler.run(),
        manager.run(),
        web_server.run(),
        bus_handler.run(),
    ]

    return coros


if __name__ == '__main__':
    asyncio.run(main())
    LOGGER.info("Stopped")


# import argparse
# import asyncio
# import logging
# import os
# import signal
#
# from typing import Optional
#
# from millegrilles_messages.MilleGrillesConnecteur import MilleGrillesConnecteur
#
# from millegrilles_streaming import Constantes
# from millegrilles_streaming.Configuration import ConfigurationStreaming
# from millegrilles_streaming.EtatStreaming import EtatStreaming
# from millegrilles_streaming.Commandes import CommandHandler
# from millegrilles_streaming.Intake import IntakeStreaming
# from millegrilles_streaming.Consignation import ConsignationHandler
# from millegrilles_streaming.WebServer import WebServer
#
# logger = logging.getLogger(__name__)
#
#
# class StreamingMain:
#
#     def __init__(self, args: argparse.Namespace):
#         self.__args = args
#         self.__config = ConfigurationStreaming()
#         self._etat = EtatStreaming(self.__config)
#
#         self.__rabbitmq_dao: Optional[MilleGrillesConnecteur] = None
#         self.__web_server: Optional[WebServer] = None
#
#         self.__commandes_handler: Optional[CommandHandler] = None
#         self.__intake: Optional[IntakeStreaming] = None
#         self.__consignation_handler: Optional[ConsignationHandler] = None
#
#         # Asyncio lifecycle handlers
#         self.__loop = None
#         self._stop_event = None
#
#     async def configurer(self):
#         self.__loop = asyncio.get_event_loop()
#         self._stop_event = asyncio.Event()
#         self.__config.parse_config(self.__args.__dict__)
#
#         await self._etat.reload_configuration()
#         self.__consignation_handler = ConsignationHandler(self._stop_event, self._etat)
#         self.__intake = IntakeStreaming(self._stop_event, self._etat, self.__consignation_handler)
#
#         self.__commandes_handler = CommandHandler(self._etat, self.__intake)
#         self.__rabbitmq_dao = MilleGrillesConnecteur(self._stop_event, self._etat, self.__commandes_handler)
#
#         await self.__intake.configurer()
#
#         # S'assurer d'avoir le repertoire de staging
#         dir_download = os.path.join(self._etat.configuration.dir_staging, Constantes.DIR_DOWNLOAD)
#         os.makedirs(dir_download, exist_ok=True)
#         dir_dechiffre = os.path.join(self._etat.configuration.dir_staging, Constantes.DIR_DECHIFFRE)
#         os.makedirs(dir_dechiffre, exist_ok=True)
#
#         self.__web_server = WebServer(self._etat, self.__commandes_handler)
#         self.__web_server.setup()
#
#     async def run(self):
#
#         threads = [
#             self.__rabbitmq_dao.run(),
#             self.__intake.run(),
#             self._etat.run(self._stop_event, self.__rabbitmq_dao),
#             self.__web_server.run(self._stop_event),
#             self.__consignation_handler.run(),
#         ]
#
#         await asyncio.gather(*threads)
#
#         logger.info("run() stopping")
#
#     def exit_gracefully(self, signum=None, frame=None):
#         logger.info("Fermer application, signal: %d" % signum)
#         self.__loop.call_soon_threadsafe(self._stop_event.set)
#
#
# def parse() -> argparse.Namespace:
#     parser = argparse.ArgumentParser(description="Demarrer le serveur de streaming pour MilleGrilles")
#     parser.add_argument(
#         '--verbose', action="store_true", required=False,
#         help="Active le logging maximal"
#     )
#
#     args = parser.parse_args()
#     adjust_logging(args)
#
#     return args
#
#
# LOGGING_NAMES = [__name__, 'millegrilles_messages', 'millegrilles_streaming']
#
#
# def adjust_logging(args: argparse.Namespace):
#     if args.verbose is True:
#         for log in LOGGING_NAMES:
#             logging.getLogger(log).setLevel(logging.DEBUG)
#
#
# async def demarrer(args: argparse.Namespace):
#     main_inst = StreamingMain(args)
#
#     signal.signal(signal.SIGINT, main_inst.exit_gracefully)
#     signal.signal(signal.SIGTERM, main_inst.exit_gracefully)
#
#     await main_inst.configurer()
#     logger.info("Run main millegrilles_streaming")
#     await main_inst.run()
#     logger.info("Fin main millegrilles_streaming")
#
#
# def main():
#     """
#     Methode d'execution de l'application
#     :return:
#     """
#     logging.basicConfig()
#     for log in LOGGING_NAMES:
#         logging.getLogger(log).setLevel(logging.INFO)
#     args = parse()
#     asyncio.run(demarrer(args))
#
#
# if __name__ == '__main__':
#     main()
