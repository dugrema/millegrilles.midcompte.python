import argparse
import asyncio
import logging
import os
import signal

from typing import Optional

from millegrilles_messages.MilleGrillesConnecteur import MilleGrillesConnecteur

from millegrilles_fichiers import Constantes
from millegrilles_fichiers.Configuration import ConfigurationFichiers
from millegrilles_fichiers.EtatFichiers import EtatFichiers
from millegrilles_fichiers.Commandes import CommandHandler
from millegrilles_fichiers.Intake import IntakeFichiers
from millegrilles_fichiers.Consignation import ConsignationHandler
from millegrilles_fichiers.WebServer import WebServer
from millegrilles_fichiers.SQLiteDao import SQLiteConnection, SQLiteLocks

logger = logging.getLogger(__name__)


class StreamingMain:

    def __init__(self, args: argparse.Namespace):
        self.__args = args
        self.__config = ConfigurationFichiers()
        self._etat = EtatFichiers(self.__config, SQLiteLocks())

        self.__rabbitmq_dao: Optional[MilleGrillesConnecteur] = None
        self.__web_server: Optional[WebServer] = None

        self.__commandes_handler: Optional[CommandHandler] = None
        self.__intake: Optional[IntakeFichiers] = None
        self.__consignation_handler: Optional[ConsignationHandler] = None

        # Asyncio lifecycle handlers
        self.__loop = None
        self._stop_event = None

    async def ainit(self):
        self.__loop = asyncio.get_event_loop()
        self._stop_event = asyncio.Event()
        self.__config.parse_config(self.__args.__dict__)

        await self._etat.ainit()
        await self._etat.reload_configuration()
        self.__consignation_handler = ConsignationHandler(self._stop_event, self._etat)
        self.__intake = IntakeFichiers(self._stop_event, self._etat, self.__consignation_handler)

        self.__commandes_handler = CommandHandler(self._etat, self.__intake, self.__consignation_handler)
        self.__rabbitmq_dao = MilleGrillesConnecteur(self._stop_event, self._etat, self.__commandes_handler)

        self.__consignation_handler.rabbitmq_dao = self.__rabbitmq_dao

        await self.__intake.configurer()

        # S'assurer d'avoir le repertoire de staging

        dir_buckets = os.path.join(self._etat.configuration.dir_consignation, Constantes.DIR_BUCKETS)
        os.makedirs(dir_buckets, exist_ok=True)

        self.__web_server = WebServer(self._etat, self.__commandes_handler, self.__intake, self.__consignation_handler)
        self.__web_server.setup()

    async def run(self):

        threads = [
            self.__rabbitmq_dao.run(),
            self.__intake.run(),
            self._etat.run(self._stop_event, self.__rabbitmq_dao),
            self.__web_server.run(self._stop_event),
            self.__consignation_handler.run(),
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


LOGGING_NAMES = [__name__, 'millegrilles_messages', 'millegrilles_fichiers']


def adjust_logging(args: argparse.Namespace):
    if args.verbose is True:
        for log in LOGGING_NAMES:
            logging.getLogger(log).setLevel(logging.DEBUG)


async def demarrer(args: argparse.Namespace):
    main_inst = StreamingMain(args)

    signal.signal(signal.SIGINT, main_inst.exit_gracefully)
    signal.signal(signal.SIGTERM, main_inst.exit_gracefully)

    await main_inst.ainit()
    logger.info("Run main millegrilles_fichiers")
    await main_inst.run()
    logger.info("Fin main millegrilles_fichiers")


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
