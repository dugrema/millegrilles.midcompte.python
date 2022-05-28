# Main script du certissuer
import argparse
import asyncio
import logging
import signal

from asyncio import Event
from asyncio.exceptions import TimeoutError
from os import makedirs
from typing import Optional

from millegrilles_certissuer.EtatCertissuer import EtatCertissuer
from millegrilles_certissuer.WebServer import WebServer
from millegrilles_certissuer.Configuration import ConfigurationCertissuer


def initialiser_certissuer():
    logging.basicConfig()

    app = CertIssuer()

    args = parse()
    if args.verbose:
        logging.getLogger('millegrilles_messages').setLevel(logging.DEBUG)
        logging.getLogger('millegrilles_certissuer').setLevel(logging.DEBUG)
    else:
        logging.getLogger('millegrilles_messages').setLevel(logging.WARN)
        logging.getLogger('millegrilles_certissuer').setLevel(logging.WARN)

    app.charger_configuration(args)

    signal.signal(signal.SIGINT, app.exit_gracefully)
    signal.signal(signal.SIGTERM, app.exit_gracefully)
    # signal.signal(signal.SIGHUP, app.reload_configuration)

    app.preparer_environnement()

    return app


def parse():
    logger = logging.getLogger(__name__ + '.parse')
    parser = argparse.ArgumentParser(description="Demarrer le certissuer")

    parser.add_argument(
        '--verbose', action="store_true", required=False,
        help="Active le logging maximal"
    )

    args = parser.parse_args()
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    logger.debug("args : %s" % args)

    return args


class CertIssuer:

    def __init__(self):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__loop = None
        self._stop_event = None  # Evenement d'arret global de l'application

        self.__configuration = ConfigurationCertissuer()
        self.__etat_certissuer: EtatCertissuer = EtatCertissuer(self.__configuration)
        self.__web_server: Optional[WebServer] = None

    def charger_configuration(self, args: argparse.Namespace):
        """
        Charge la configuration d'environnement (os.env, /var/opt/millegrilles/instance)
        :return:
        """
        self.__logger.info("Charger la configuration")
        self.__configuration.parse_config(args)

    def preparer_environnement(self):
        """
        Examine environnement, preparer au besoin (folders, docker, ports, etc)
        :return:
        """
        self.__logger.info("Preparer l'environnement")
        # makedirs(self.__configuration.path_secrets, 0o700, exist_ok=True)

        self.preparer_folder_configuration()

        self.__web_server = WebServer(self.__etat_certissuer)
        self.__web_server.setup()

    def preparer_folder_configuration(self):
        makedirs(self.__configuration.path_certissuer, 0o700, exist_ok=True)

    def exit_gracefully(self, signum=None, frame=None):
        self.__logger.info("Fermer application, signal: %d" % signum)
        self.fermer()

    async def entretien(self):
        """
        Entretien du systeme. Invoque a intervalle regulier.
        :return:
        """
        self.__logger.debug("Debut cycle d'entretien")

        while self._stop_event.is_set() is False:
            # Entretien

            try:
                # Attente 30 secondes entre entretien
                await asyncio.wait_for(self._stop_event.wait(), 30)
            except TimeoutError:
                pass

        self.__logger.debug("Fin cycle d'entretien")

    def fermer(self):
        if self.__loop is not None:
            self.__loop.call_soon_threadsafe(self._stop_event.set)

    async def executer(self):
        """
        Boucle d'execution principale
        :return:
        """
        self.__loop = asyncio.get_event_loop()
        self._stop_event = Event()

        await self.__etat_certissuer.charger_init()

        tasks = [
            asyncio.create_task(self.entretien()),
            asyncio.create_task(self.__web_server.run(self._stop_event))
        ]

        # Execution de la loop avec toutes les tasks
        await asyncio.tasks.wait(tasks, return_when=asyncio.tasks.FIRST_COMPLETED)


def main():
    """
    Methode d'execution du certissuer
    :return:
    """
    app = initialiser_certissuer()
    logger = logging.getLogger(__name__)

    try:
        logger.info("Debut execution certissuer")
        asyncio.run(app.executer())
        logger.info("Fin execution certissuer")
    except KeyboardInterrupt:
        logger.info("Arret execution app via signal (KeyboardInterrupt), fin thread main")
    except:
        logger.exception("Exception durant execution app, fin thread main")
    finally:
        app.fermer()  # S'assurer de mettre le flag de stop_event


if __name__ == '__main__':
    main()
