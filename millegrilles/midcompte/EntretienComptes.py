import asyncio
import logging

from typing import Optional

from asyncio import Event, TimeoutError

from millegrilles.midcompte.Configuration import ConfigurationMidcompte


class ModuleEntretienComptes:

    def __init__(self, configuration: ConfigurationMidcompte):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__configuration = configuration

    async def run(self, stop_event: Event):
        self.__logger.info("run Debut")

        while stop_event.is_set() is False:
            self.__logger.debug("run() debut cycle entretien")

            try:
                self.__logger.debug("run() fin cycle entretien")
                await asyncio.wait_for(stop_event.wait(), 30)
            except TimeoutError:
                pass

        self.__logger.info("run Fin")
