import argparse
import asyncio
import logging
import signal

from typing import Optional

from millegrilles_media.mqdao import RabbitMQDao

from millegrilles_media.Configuration import ConfigurationMedia
from millegrilles_media.Commandes import CommandHandler
from millegrilles_media.EtatMedia import EtatMedia
from millegrilles_media.intake import IntakeJobImage, IntakeJobVideo

logger = logging.getLogger(__name__)


class ESMain:

    def __init__(self, args: argparse.Namespace):
        self.__args = args
        self.__config = ConfigurationMedia()
        self._etat_media = EtatMedia(self.__config)

        self.__rabbitmq_dao: Optional[RabbitMQDao] = None

        self.__commandes_handler = None
        self.__intake = None

        # Asyncio lifecycle handlers
        self.__loop = None
        self._stop_event = None

    async def configurer(self):
        self.__loop = asyncio.get_event_loop()
        self._stop_event = asyncio.Event()
        self.__config.parse_config(self.__args.__dict__)

        await self._etat_media.reload_configuration()
        self.__intake_images = IntakeJobImage(self._stop_event, self._etat_media)
        self.__intake_videos = IntakeJobVideo(self._stop_event, self._etat_media)
        self.__commandes_handler = CommandHandler(self._etat_media, self.__intake_images, self.__intake_videos)
        self.__rabbitmq_dao = RabbitMQDao(self._stop_event, self._etat_media, self.__commandes_handler)

        await self.__intake_images.configurer()
        await self.__intake_videos.configurer()

    async def run(self):
        await asyncio.gather(
            self.__rabbitmq_dao.run(),
            self.__intake_images.run(),
            self.__intake_videos.run(),
            self._etat_media.run(self._stop_event, self.__rabbitmq_dao),
        )

        logger.info("run() stopping")

    async def run_scripts(self):
        import json
        # await self.__solrdao.ping()

        # Debug
        #await self.__solrdao.list_field_types()
        #await self.__solrdao.preparer_sample_data()
        #await self.__solrdao.preparer_sample_file()
        resultat = await self.__requetes_handler.requete_fichiers('z2i3Xjx8abNcGbqKFa5bNzR3UGJkLWUBSgn5c6yZRQW6TxtdDPE', 'abus physiques')
        logger.info("Resultat requete \n%s" % json.dumps(resultat, indent=2))

    def exit_gracefully(self, signum=None, frame=None):
        logger.info("Fermer application, signal: %d" % signum)
        self.__loop.call_soon_threadsafe(self._stop_event.set)


def parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Demarrer le gestionnaire de taches media pour MilleGrilles")
    parser.add_argument(
        '--verbose', action="store_true", required=False,
        help="Active le logging maximal"
    )
    parser.add_argument(
        '--scripts', action="store_true", required=False,
        help="Execute scripts"
    )

    args = parser.parse_args()
    adjust_logging(args)

    return args


def adjust_logging(args: argparse.Namespace):
    if args.verbose is True:
        loggers = [__name__, 'millegrilles_messages', 'millegrilles_media', 'mediadao']
        for log in loggers:
            logging.getLogger(log).setLevel(logging.DEBUG)


async def demarrer(args: argparse.Namespace):
    main = ESMain(args)

    signal.signal(signal.SIGINT, main.exit_gracefully)
    signal.signal(signal.SIGTERM, main.exit_gracefully)

    await main.configurer()
    if args.scripts is True:
        logger.info("Run main millegrilles_media scripts")
        await main.run_scripts()
    else:
        logger.info("Run main millegrilles_media")
        await main.run()
    logger.info("Fin main millegrilles_media")


def main():
    """
    Methode d'execution de l'application
    :return:
    """
    logging.basicConfig()
    logging.getLogger(__name__).setLevel(logging.INFO)
    logging.getLogger('mediadao').setLevel(logging.INFO)
    logging.getLogger('millegrilles_media').setLevel(logging.INFO)
    args = parse()
    asyncio.run(demarrer(args))


if __name__ == '__main__':
    main()
