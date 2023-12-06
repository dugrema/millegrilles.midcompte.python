import argparse
import asyncio
import logging
import signal

from aiohttp.client_exceptions import ClientResponseError
from typing import Optional

from millegrilles_solr.mqdao import RabbitMQDao

from millegrilles_solr.solrdao import SolrDao
from millegrilles_solr.Configuration import ConfigurationRelaiSolr
from millegrilles_solr.Commandes import CommandHandler
from millegrilles_solr.EtatRelaiSolr import EtatRelaiSolr
from millegrilles_solr.requetes import RequetesHandler
from millegrilles_solr.intake import IntakeHandler

logger = logging.getLogger(__name__)


class ESMain:

    def __init__(self, args: argparse.Namespace):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__args = args
        self.__config = ConfigurationRelaiSolr()
        self._etat_relaisolr = EtatRelaiSolr(self.__config)

        self.__solrdao = SolrDao(self._etat_relaisolr)
        self.__rabbitmq_dao: Optional[RabbitMQDao] = None

        self.__requetes_handler = RequetesHandler(self._etat_relaisolr, self.__solrdao)
        self.__commandes_handler = None
        self.__intake = None

        # Asyncio lifecycle handlers
        self.__loop = None
        self._stop_event = None

    async def configurer(self):
        self.__loop = asyncio.get_event_loop()
        self._stop_event = asyncio.Event()
        self.__config.parse_config(self.__args.__dict__)

        await self._etat_relaisolr.reload_configuration()
        self.__intake = IntakeHandler(self._stop_event, self._etat_relaisolr, self.__solrdao)
        self.__commandes_handler = CommandHandler(self._etat_relaisolr, self.__requetes_handler, self.__intake)
        self.__rabbitmq_dao = RabbitMQDao(self._stop_event, self._etat_relaisolr, self.__commandes_handler)

        self.__solrdao.configure()
        await self.__intake.configurer()

        # Configurer core1
        try:
            await self.__solrdao.ping()
        except ClientResponseError as e:
            if e.status == 400:
                self.__logger.fatal("Erreur connexion a SOLR, certificat HTTPS rejete")
            raise e
        await self.__solrdao.initialiser_solr()

    async def run(self):
        await self.__solrdao.ping()

        await asyncio.gather(
            self.__rabbitmq_dao.run(),
            self.__intake.run(),
            self._etat_relaisolr.run(self._stop_event, self.__rabbitmq_dao),
        )

        logger.info("run() stopping")

    async def run_scripts(self):
        import json
        # await self.__solrdao.ping()

        # Debug
        pass
        #await self.__solrdao.list_field_types()
        #await self.__solrdao.preparer_sample_data()
        #await self.__solrdao.preparer_sample_file()
        #resultat = await self.__requetes_handler.requete_fichiers('z2i3Xjx8abNcGbqKFa5bNzR3UGJkLWUBSgn5c6yZRQW6TxtdDPE', 'abus physiques')
        # resultat = await self.__requetes_handler.requete_fichiers('z2i3XjxE6PXsVKYy6BUzAkxv7HfZHrzmKVTZsyEJvxzpmFNjtwx', '001')
        # logger.info("Resultat requete \n%s" % json.dumps(resultat, indent=2))
        # await self.__solrdao.reset_index(self.__config.nom_collection_fichiers, delete=True)

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
        loggers = [__name__, 'millegrilles_messages', 'millegrilles_solr', 'solrdao']
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
    logging.getLogger('millegrilles_solr').setLevel(logging.INFO)
    logging.getLogger('millegrilles_messages').setLevel(logging.INFO)
    args = parse()
    asyncio.run(demarrer(args))


if __name__ == '__main__':
    main()
