import argparse
import asyncio
import logging
import os
import signal

from typing import Optional

from millegrilles_ceduleur.mqdao import RabbitMQDao

from millegrilles_ceduleur.Configuration import ConfigurationCeduleur
from millegrilles_ceduleur.EtatCeduleur import EtatCeduleur
from millegrilles_ceduleur.Commandes import CommandHandler
from millegrilles_ceduleur.EmetteurCedule import Ceduleur

logger = logging.getLogger(__name__)


class ESMain:

    def __init__(self, args: argparse.Namespace):
        self.__args = args
        self.__config = ConfigurationCeduleur()
        self._etat_ceduleur = EtatCeduleur(self.__config)

        self.__rabbitmq_dao: Optional[RabbitMQDao] = None

        self.__commandes_handler = None

        self.__ceduleur: Optional[Ceduleur] = None

        # Asyncio lifecycle handlers
        self.__loop = None
        self._stop_event = None

    async def configurer(self):
        self.__loop = asyncio.get_event_loop()
        self._stop_event = asyncio.Event()
        self.__config.parse_config(self.__args.__dict__)

        await self._etat_ceduleur.reload_configuration()
        self.__commandes_handler = CommandHandler(self._etat_ceduleur)
        self.__rabbitmq_dao = RabbitMQDao(self._stop_event, self._etat_ceduleur, self.__commandes_handler)
        self.__ceduleur = Ceduleur(self._etat_ceduleur, self._stop_event)

    async def run(self):

        ceduleur = Ceduleur(self._etat_ceduleur, self._stop_event)
        threads = [
            self.__rabbitmq_dao.run(),
            self._etat_ceduleur.run(self._stop_event, self.__rabbitmq_dao),
            ceduleur.run(),
        ]

        await asyncio.gather(*threads)

        logger.info("run() stopping")

    def exit_gracefully(self, signum=None, frame=None):
        logger.info("Fermer application, signal: %d" % signum)
        self.__loop.call_soon_threadsafe(self._stop_event.set)


def parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Demarrer le gestionnaire de taches media pour MilleGrilles")
    parser.add_argument(
        '--verbose', action="store_true", required=False,
        help="Active le logging maximal"
    )

    args = parser.parse_args()
    adjust_logging(args)

    return args


def adjust_logging(args: argparse.Namespace):
    if args.verbose is True:
        loggers = [__name__, 'millegrilles_messages', 'millegrilles_ceduleur']
        for log in loggers:
            logging.getLogger(log).setLevel(logging.DEBUG)


async def demarrer(args: argparse.Namespace):
    main = ESMain(args)

    signal.signal(signal.SIGINT, main.exit_gracefully)
    signal.signal(signal.SIGTERM, main.exit_gracefully)

    await main.configurer()
    logger.info("Run main millegrilles_ceduleur")
    await main.run()
    logger.info("Fin main millegrilles_ceduleur")


def main():
    """
    Methode d'execution de l'application
    :return:
    """
    logging.basicConfig()
    logging.getLogger(__name__).setLevel(logging.INFO)
    logging.getLogger('millegrilles_ceduleur').setLevel(logging.INFO)
    args = parse()
    asyncio.run(demarrer(args))


if __name__ == '__main__':
    main()
