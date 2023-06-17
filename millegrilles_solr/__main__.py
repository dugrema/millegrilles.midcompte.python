import argparse
import asyncio
import logging
import signal

from typing import Optional

from millegrilles_relaiweb.RabbitMQDao import RabbitMQDao

from solrdao import SolrDao
from Configuration import ConfigurationRelaiSolr
from millegrilles_solr.Commandes import CommandHandler
from millegrilles_solr.EtatRelaiSolr import EtatRelaiSolr

logger = logging.getLogger(__name__)


class ESMain:

    def __init__(self, args: argparse.Namespace):
        self.__args = args
        self.__config = ConfigurationRelaiSolr()
        self._etat_relaisolr = EtatRelaiSolr(self.__config)
        self._commandes_handler = CommandHandler(self._etat_relaisolr)

        self.__solrdao = SolrDao(self._etat_relaisolr)
        self.__rabbitmq_dao: Optional[RabbitMQDao] = None

        # Asyncio lifecycle handlers
        self.__loop = None
        self._stop_event = None

    async def configurer(self):
        self.__loop = asyncio.get_event_loop()
        self._stop_event = asyncio.Event()
        self.__config.parse_config(self.__args.__dict__)

        self.__rabbitmq_dao = RabbitMQDao(self._stop_event, self._etat_relaisolr)

        self.__solrdao.configure(
            '/var/opt/millegrilles/secrets/pki.core.cert',
            '/var/opt/millegrilles/secrets/pki.core.cle',
            '/var/opt/millegrilles/configuration/pki.millegrille.cert'
        )

        # Configurer core1
        await self.__solrdao.initialiser_solr()

    async def run(self):
        await self.__solrdao.ping()
        raise NotImplementedError('todo')

    async def run_scripts(self):
        # await self.__solrdao.ping()

        # Debug
        #await self.__solrdao.list_field_types()
        # await self.__solrdao.preparer_sample_data()
        await self.__solrdao.preparer_sample_file()
        #await self.__solrdao.requete()

    def exit_gracefully(self, signum=None, frame=None):
        logger.info("Fermer application, signal: %d" % signum)
        self.__loop.call_soon_threadsafe(self._stop_event.set)


def parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Demarrer le gestionnaire de taches SOLR pour MilleGrilles")
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
        loggers = [__name__, 'millegrilles_messages', 'solrdao']
        for log in loggers:
            logging.getLogger(log).setLevel(logging.DEBUG)


async def demarrer(args: argparse.Namespace):
    main = ESMain(args)

    signal.signal(signal.SIGINT, main.exit_gracefully)
    signal.signal(signal.SIGTERM, main.exit_gracefully)

    await main.configurer()
    if args.scripts is True:
        logger.info("Run main millegrilles_solr scripts")
        await main.run_scripts()
    else:
        logger.info("Run main millegrilles_solr")
        await main.run()
    logger.info("Fin main millegrilles_solr")


def main():
    """
    Methode d'execution de l'application
    :return:
    """
    logging.basicConfig()
    logging.getLogger(__name__).setLevel(logging.INFO)
    logging.getLogger('solrdao').setLevel(logging.INFO)
    args = parse()
    asyncio.run(demarrer(args))


if __name__ == '__main__':
    main()
