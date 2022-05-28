import aiohttp
import logging
import ssl

from os import path
from typing import Optional

from aiohttp.client_exceptions import ClientConnectorError


class EntretienMongoDb:

    def __init__(self, etat_midcompte):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__etat_midcompte = etat_midcompte

        self.__passwd_mongodb: Optional[str] = None

        self.__entretien_initial_complete = False

    async def entretien(self):
        self.__logger.debug("entretien debut")

        try:
            pass
        except Exception as e:
            self.__logger.exception("Erreur verification RabbitMQ https")

        self.__logger.debug("entretien fin")

    async def ajouter_compte(self, info: dict):
        self.__logger.debug("Ajouter compte dans MongoDB: %s" % info)
        raise NotImplementedError('todo')
