import argparse
import asyncio
import datetime
import logging
import subprocess
import tarfile
import json
import requests
import signal

from os import listdir, path, unlink, makedirs
from typing import Optional

from millegrilles_messages.docker.Entretien import TacheEntretien
from millegrilles_messages.messages import Constantes
from millegrilles_messages.messages.EnveloppeCertificat import EnveloppeCertificat
from millegrilles_messages.messages.FormatteurMessages import SignateurTransactionSimple, FormatteurMessageMilleGrilles
from millegrilles_messages.messages.CleCertificat import CleCertificat
from millegrilles_relaiweb.Configuration import ConfigurationRelaiWeb
from millegrilles_relaiweb.Commandes import CommandHandler
from millegrilles_relaiweb.EtatRelaiWeb import EtatRelaiWeb
from millegrilles_relaiweb.RabbitMQDao import RabbitMQDao

TAILLE_BUFFER = 32 * 1024


class RelaiWeb:

    def __init__(self):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__config = ConfigurationRelaiWeb()

        self.__enveloppe_ca: Optional[EnveloppeCertificat] = None
        self.__formatteur: Optional[FormatteurMessageMilleGrilles] = None

        self.__loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop_event: Optional[asyncio.Event] = None  # Evenement d'arret global de l'application
        self.__rabbitmq_dao: Optional[RabbitMQDao] = None

        self._etat_relaiweb = EtatRelaiWeb(self.__config)
        self._commandes_handler = CommandHandler(self._etat_relaiweb, )

        self.__taches = self.preparer_taches()

    async def charger_configuration(self, args: argparse.Namespace):
        """
        Charge la configuration d'environnement (os.env)
        :return:
        """
        self.__logger.info("Charger la configuration")
        self.__loop = asyncio.get_event_loop()
        self._stop_event = asyncio.Event()
        self.__config.parse_config(args.__dict__)

        # self._etat_relaiweb.ajouter_listener(self._senseur_modules_handler.reload_configuration)
        await self._etat_relaiweb.reload_configuration()

        self.__rabbitmq_dao = RabbitMQDao(self._stop_event, self._etat_relaiweb)

        self.__logger.info("charger_configuration prete")

    def preparer_taches(self) -> list:
        taches = list()
        # taches.append(TacheEntretien(datetime.timedelta(minutes=30), self.rotation_logs))
        # taches.append(TacheEntretien(datetime.timedelta(minutes=5), self.verifier_expirations))
        return taches

    async def fermer(self):
        self._stop_event.set()

    async def __attendre_fermer(self):
        await self._stop_event.wait()
        self.__logger.info("executer __attendre_fermer")
        await self.fermer()

    def exit_gracefully(self, signum=None, frame=None):
        self.__logger.info("Fermer application, signal: %d" % signum)
        self.__loop.call_soon_threadsafe(self._stop_event.set)

    async def entretien(self):
        self.__logger.info("entretien thread debut")

        while self._stop_event.is_set() is False:
            self.__logger.debug("run() debut execution cycle")

            for tache in self.__taches:
                try:
                    await tache.run()
                except Exception:
                    self.__logger.exception("Erreur execution tache")

            try:
                self.__logger.debug("run() fin execution cycle")
                await asyncio.wait_for(self._stop_event.wait(), 10)
            except asyncio.TimeoutError:
                pass

        self.__logger.info("entretien thread fin")

    async def run(self):
        """
        Boucle d'execution principale
        :return:
        """

        tasks = [
            asyncio.create_task(self.entretien(), name="entretien"),
            asyncio.create_task(self.__rabbitmq_dao.run(), name="mq"),
            # asyncio.create_task(self._senseur_modules_handler.run(), name="senseur_modules"),
            self.__attendre_fermer()
        ]

        # Execution de la loop avec toutes les tasks
        try:
            await asyncio.tasks.wait(tasks, return_when=asyncio.tasks.FIRST_COMPLETED)
        finally:
            await self.fermer()

    def relai_web(self, commande: dict):
        self.__logger.debug("Commande relai web : %s", commande)
        try:
            certificat = commande['certificat']
        except (AttributeError, KeyError):
            return {'ok': False, 'code': 400, 'err': 'Certificat absent'}

        exchanges = certificat.get_exchanges
        if Constantes.SECURITE_PUBLIC in exchanges:
            contenu = commande

            params = {
                'url': contenu['url'],
                'timeout': contenu.get('timeout') or 20,
            }

            # Copier parametres optionnels
            params_optionnels = ['headers', 'data', 'json']
            for nom_param in params_optionnels:
                if contenu.get(nom_param) is not None:
                    params[nom_param] = contenu[nom_param]

            method: str = contenu.get('method') or 'GET'
            flag_erreur_https = False
            if method.lower() == 'get':
                try:
                    response = requests.get(**params)
                except requests.exceptions.SSLError:
                    self.__logger.debug("Erreur certificat https, ajouter un flag certificat invalide")
                    flag_erreur_https = True
                    params['verify'] = False  # Desactiver verification certificat https
                    response = requests.get(**params)
                except requests.exceptions.ReadTimeout:
                    self.__logger.error("Erreur timeout sur %s", params['url'])
                    return {'ok': False, 'code': 408, 'err': 'Methode inconnue'}
            elif method.lower() == 'post':
                response = requests.post(**params)
            else:
                return {'ok': False, 'code': 400, 'err': 'Methode inconnue'}

            self.__logger.debug("Response : %s" % response)

            if 200 <= response.status_code < 300:
                headers = response.headers
                header_dict = {}
                for header_key in headers.keys():
                    header_dict[header_key] = headers.get(header_key)
                try:
                    json_response = response.json()
                    return {'headers': header_dict, 'json': json_response, 'code': response.status_code, 'verify_ok': not flag_erreur_https}
                except:
                    # Encoder reponse en multibase
                    return {'headers': header_dict, 'text': response.text, 'code': response.status_code, 'verify_ok': not flag_erreur_https}
            else:
                # Erreur
                return {'ok': False, 'code': response.status_code, 'err': response.text}

        else:
            return {'ok': False, 'code': 403, 'err': 'Not authorized'}
