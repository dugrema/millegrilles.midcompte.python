import argparse
import asyncio
import logging
import os
import signal

from typing import Optional

from millegrilles_messages.MilleGrillesConnecteur import MilleGrillesConnecteur

from millegrilles_backup.Configuration import ConfigurationBackup
from millegrilles_backup.Commandes import CommandHandler
from millegrilles_backup.Intake import IntakeBackup
from millegrilles_backup.Restauration import HandlerRestauration
from millegrilles_backup.Consignation import ConsignationHandler
from millegrilles_backup.EtatBackup import EtatBackup

logger = logging.getLogger(__name__)


class BackupMain:

    def __init__(self, args: argparse.Namespace):
        self.__args = args
        self.__config = ConfigurationBackup()
        self._etat_backup = EtatBackup(self.__config)

        self.__rabbitmq_dao: Optional[MilleGrillesConnecteur] = None

        self.__commandes_handler: Optional[CommandHandler] = None
        self.__intake_backups: Optional[IntakeBackup] = None
        self.__restauration_handler: Optional[HandlerRestauration] = None
        self.__consignation_handler: Optional[ConsignationHandler] = None

        # Asyncio lifecycle handlers
        self.__loop = None
        self._stop_event = None

    async def configurer(self):
        self.__loop = asyncio.get_event_loop()
        self._stop_event = asyncio.Event()
        self.__config.parse_config(self.__args.__dict__)

        await self._etat_backup.reload_configuration()
        self.__intake_backups = IntakeBackup(self._stop_event, self._etat_backup, triggers_complete=[self.trigger_backup_complete])
        self.__restauration_handler = HandlerRestauration(self._stop_event, self._etat_backup)
        self.__consignation_handler = ConsignationHandler(self._stop_event, self._etat_backup)

        self.__commandes_handler = CommandHandler(self._etat_backup, self.__intake_backups, self.__restauration_handler)
        self.__rabbitmq_dao = MilleGrillesConnecteur(self._stop_event, self._etat_backup, self.__commandes_handler)

        await self.__intake_backups.configurer()
        await self.__consignation_handler.configurer()

        # S'assurer d'avoir le repertoire de staging
        dir_backup = self._etat_backup.configuration.dir_backup
        os.makedirs(dir_backup, exist_ok=True)

    async def trigger_backup_complete(self):
        await self.__consignation_handler.trigger()

    async def run(self):

        threads = [
            self.__rabbitmq_dao.run(),
            self.__intake_backups.run(),
            self.__restauration_handler.run(),
            self.__consignation_handler.run(),
            self._etat_backup.run(self._stop_event, self.__rabbitmq_dao),
        ]

        await asyncio.gather(*threads)

        logger.info("run() stopping")

    def exit_gracefully(self, signum=None, frame=None):
        logger.info("Fermer application, signal: %d" % signum)
        self.__loop.call_soon_threadsafe(self._stop_event.set)


def parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Demarrer le gestionnaire de backup pour MilleGrilles")
    parser.add_argument(
        '--verbose', action="store_true", required=False,
        help="Active le logging maximal"
    )

    args = parser.parse_args()
    adjust_logging(args)

    return args


LOGGING_NAMES = [__name__, 'millegrilles_messages', 'millegrilles_backup']


def adjust_logging(args: argparse.Namespace):
    if args.verbose is True:
        for log in LOGGING_NAMES:
            logging.getLogger(log).setLevel(logging.DEBUG)


async def demarrer(args: argparse.Namespace):
    main = BackupMain(args)

    signal.signal(signal.SIGINT, main.exit_gracefully)
    signal.signal(signal.SIGTERM, main.exit_gracefully)

    await main.configurer()
    logger.info("Run main millegrilles_backup")
    await main.run()
    logger.info("Fin main millegrilles_backup")


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
