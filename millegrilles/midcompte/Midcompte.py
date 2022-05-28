import argparse
import asyncio
import datetime
import logging
import signal

from asyncio import Event, AbstractEventLoop, TimeoutError
from typing import Optional

from millegrilles.midcompte.Configuration import ConfigurationMidcompte
from millegrilles.midcompte.EntretienComptes import ModuleEntretienComptes
from millegrilles.midcompte.EtatMidcompte import EtatMidcompte
from millegrilles.midcompte.EntretienRabbitMq import EntretienRabbitMq
from millegrilles.midcompte.EntretienMongoDb import EntretienMongoDb
from millegrilles.docker.Entretien import TacheEntretien
from millegrilles.midcompte.WebServer import WebServer


class ApplicationInstance:

    def __init__(self):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

        self.__configuration = ConfigurationMidcompte()
        self.__etat_midcompte = EtatMidcompte(self.__configuration)

        self.__web_server: Optional[WebServer] = None

        self.__module_entretien_comptes: Optional[ModuleEntretienComptes] = None
        self.__module_entretien_mongodb: Optional[EntretienMongoDb] = None
        self.__module_entretien_rabbitmq: Optional[EntretienRabbitMq] = None

        self.__tache_comptes: Optional = TacheEntretien(datetime.timedelta(seconds=30), self.entretien_comptes)
        self.__tache_mongodb = TacheEntretien(datetime.timedelta(seconds=30), self.entretien_mongodb)
        self.__tache_rabbitmq = TacheEntretien(datetime.timedelta(seconds=30), self.entretien_rabbitmq)

        self.__loop: Optional[AbstractEventLoop] = None
        self._stop_event: Optional[Event] = None  # Evenement d'arret global de l'application

    async def charger_configuration(self, args: argparse.Namespace):
        """
        Charge la configuration d'environnement (os.env)
        :return:
        """
        self.__logger.info("Charger la configuration")
        self.__loop = asyncio.get_event_loop()
        self._stop_event = Event()
        self.__configuration.parse_config(args)
        await self.__etat_midcompte.reload_configuration()

        self.__module_entretien_comptes = ModuleEntretienComptes(self.__etat_midcompte)
        self.__module_entretien_mongodb = EntretienMongoDb(self.__etat_midcompte)
        self.__module_entretien_rabbitmq = EntretienRabbitMq(self.__etat_midcompte)

        # self.__etat_midcompte.ajouter_listener(self.__module_entretien_mongodb)
        self.__etat_midcompte.ajouter_listener(self.__module_entretien_rabbitmq)

        self.__logger.info("charger_configuration prete")

    def exit_gracefully(self, signum=None, frame=None):
        self.__logger.info("Fermer application, signal: %d" % signum)
        self.__loop.call_soon_threadsafe(self._stop_event.set)

    async def fermer(self):
        self._stop_event.set()

    async def entretien_comptes(self):
        self.__logger.debug("entretien_comptes")

    async def entretien_mongodb(self):
        self.__logger.debug("entretien_mongodb")

    async def entretien_rabbitmq(self):
        self.__logger.debug("entretien_rabbitmq")
        await self.__module_entretien_rabbitmq.entretien()
        if self.__module_entretien_rabbitmq.entretien_initial_complete is True:
            # Changer periode entretien a 60 minutes
            self.__logger.debug("RabbitMQ initialise OK - Changer periode d'entretien rabbitmq a 60 minutes")
            self.__tache_rabbitmq.set_intervalle(datetime.timedelta(minutes=60))

    async def entretien(self):
        self.__logger.info("entretien thread debut")

        while self._stop_event.is_set() is False:
            self.__logger.debug("run() debut execution cycle")

            await self.__tache_rabbitmq.run()
            await self.__tache_mongodb.run()
            await self.__tache_comptes.run()

            try:
                self.__logger.debug("run() fin execution cycle")
                await asyncio.wait_for(self._stop_event.wait(), 10)
            except TimeoutError:
                pass

        self.__logger.info("entretien thread fin")

    async def preparer_environnement(self):
        self.__web_server = WebServer(self.__etat_midcompte)
        self.__web_server.setup()

    async def executer(self):
        """
        Boucle d'execution principale
        :return:
        """

        tasks = [
            asyncio.create_task(self.entretien()),
            asyncio.create_task(self.__web_server.run(self._stop_event))
        ]

        # Execution de la loop avec toutes les tasks
        await asyncio.tasks.wait(tasks, return_when=asyncio.tasks.FIRST_COMPLETED)


async def initialiser_application():
    logging.basicConfig()

    app = ApplicationInstance()

    args = parse()
    if args.verbose:
        logging.getLogger('millegrilles').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)
    else:
        logging.getLogger('millegrilles').setLevel(logging.WARN)

    await app.charger_configuration(args)

    signal.signal(signal.SIGINT, app.exit_gracefully)
    signal.signal(signal.SIGTERM, app.exit_gracefully)
    # signal.signal(signal.SIGHUP, app.reload_configuration)

    return app


def parse():
    logger = logging.getLogger(__name__ + '.parse')
    parser = argparse.ArgumentParser(description="Demarrer l'application d'instance MilleGrilles")

    parser.add_argument(
        '--verbose', action="store_true", required=False,
        help="Active le logging maximal"
    )

    args = parser.parse_args()
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    logger.debug("args : %s" % args)

    return args


async def demarrer():
    logger = logging.getLogger(__name__)

    logger.info("Setup app")
    app = await initialiser_application()
    await app.preparer_environnement()

    try:
        logger.info("Debut execution app")
        await app.executer()
    except KeyboardInterrupt:
        logger.info("Arret execution app via signal (KeyboardInterrupt), fin thread main")
    except:
        logger.exception("Exception durant execution app, fin thread main")
    finally:
        logger.info("Fin execution app")
        await app.fermer()  # S'assurer de mettre le flag de stop_event


def main():
    """
    Methode d'execution de l'application
    :return:
    """
    asyncio.run(demarrer())


if __name__ == '__main__':
    main()
