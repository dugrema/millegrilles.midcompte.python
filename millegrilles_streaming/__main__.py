import argparse
import asyncio
import logging
import os
import signal

from typing import Optional

from millegrilles_messages.MilleGrillesConnecteur import MilleGrillesConnecteur

from millegrilles_streaming import Constantes
from millegrilles_streaming.Configuration import ConfigurationStreaming
from millegrilles_streaming.EtatStreaming import EtatStreaming
from millegrilles_streaming.Commandes import CommandHandler
from millegrilles_streaming.Intake import IntakeStreaming
from millegrilles_streaming.Consignation import ConsignationHandler
from millegrilles_streaming.WebServer import WebServer

logger = logging.getLogger(__name__)


class StreamingMain:

    def __init__(self, args: argparse.Namespace):
        self.__args = args
        self.__config = ConfigurationStreaming()
        self._etat = EtatStreaming(self.__config)

        self.__rabbitmq_dao: Optional[MilleGrillesConnecteur] = None
        self.__web_server: Optional[WebServer] = None

        self.__commandes_handler: Optional[CommandHandler] = None
        self.__intake: Optional[IntakeStreaming] = None
        self.__consignation_handler: Optional[ConsignationHandler] = None

        # Asyncio lifecycle handlers
        self.__loop = None
        self._stop_event = None

    async def configurer(self):
        self.__loop = asyncio.get_event_loop()
        self._stop_event = asyncio.Event()
        self.__config.parse_config(self.__args.__dict__)

        await self._etat.reload_configuration()
        self.__intake = IntakeStreaming(self._stop_event, self._etat)
        self.__consignation_handler = ConsignationHandler(self._stop_event, self._etat)

        self.__commandes_handler = CommandHandler(self._etat, self.__intake)
        self.__rabbitmq_dao = MilleGrillesConnecteur(self._stop_event, self._etat, self.__commandes_handler)

        await self.__intake.configurer()
        await self.__consignation_handler.configurer()

        # S'assurer d'avoir le repertoire de staging
        dir_download = os.path.join(self._etat.configuration.dir_staging, Constantes.DIR_DOWNLOAD)
        os.makedirs(dir_download, exist_ok=True)
        dir_dechiffre = os.path.join(self._etat.configuration.dir_staging, Constantes.DIR_DECHIFFRE)
        os.makedirs(dir_dechiffre, exist_ok=True)

        self.__web_server = WebServer(self._etat, self.__commandes_handler)
        self.__web_server.setup()

    async def run(self):

        threads = [
            self.__rabbitmq_dao.run(),
            self.__intake.run(),
            self.__consignation_handler.run(),
            self._etat.run(self._stop_event, self.__rabbitmq_dao),
            self.__web_server.run(self._stop_event),
        ]

        await asyncio.gather(*threads)

        logger.info("run() stopping")

    def exit_gracefully(self, signum=None, frame=None):
        logger.info("Fermer application, signal: %d" % signum)
        self.__loop.call_soon_threadsafe(self._stop_event.set)


def parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Demarrer le serveur de streaming pour MilleGrilles")
    parser.add_argument(
        '--verbose', action="store_true", required=False,
        help="Active le logging maximal"
    )

    args = parser.parse_args()
    adjust_logging(args)

    return args


LOGGING_NAMES = [__name__, 'millegrilles_messages', 'millegrilles_streaming']


def adjust_logging(args: argparse.Namespace):
    if args.verbose is True:
        for log in LOGGING_NAMES:
            logging.getLogger(log).setLevel(logging.DEBUG)


async def demarrer(args: argparse.Namespace):
    main_inst = StreamingMain(args)

    signal.signal(signal.SIGINT, main_inst.exit_gracefully)
    signal.signal(signal.SIGTERM, main_inst.exit_gracefully)

    await main_inst.configurer()
    logger.info("Run main millegrilles_streaming")
    await main_inst.run()
    logger.info("Fin main millegrilles_streaming")


def main():
    """
    Methode d'execution de l'application
    :return:
    """
    logging.basicConfig()
    for log in LOGGING_NAMES:
        logging.getLogger(log).setLevel(logging.INFO)
    args = parse()
    asyncio.run(demarrer(args))


if __name__ == '__main__':
    main()
