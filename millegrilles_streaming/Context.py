import aiohttp
import logging
import pathlib
import ssl

from typing import Optional, Callable
from urllib.parse import urlparse

from millegrilles_messages.bus.BusContext import MilleGrillesBusContext
from millegrilles_messages.bus.PikaConnector import MilleGrillesPikaConnector
from millegrilles_messages.structs.Filehost import Filehost
from millegrilles_streaming.Configuration import StreamingConfiguration


class StreamingContext(MilleGrillesBusContext):

    def __init__(self, configuration: StreamingConfiguration):
        self.__reload_listeners: list[Callable[[], None]] = list()
        super().__init__(configuration)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__bus_connector: Optional[MilleGrillesPikaConnector] = None

    def reload(self):
        super().reload()
        for listener in self.__reload_listeners:
            listener()

    def add_reload_listener(self, listener: Callable[[], None]):
        self.__reload_listeners.append(listener)

    @property
    def configuration(self) -> StreamingConfiguration:
        return super().configuration

    def get_http_session(self, timeout: Optional[aiohttp.ClientTimeout] = None) -> aiohttp.ClientSession:
        connector = self.get_tcp_connector()
        return aiohttp.ClientSession(timeout=timeout, connector=connector)

    @property
    def download_path(self) -> pathlib.Path:
        return pathlib.Path(self.configuration.dir_staging, 'download')

    @property
    def decrypted_path(self) -> pathlib.Path:
        return pathlib.Path(self.configuration.dir_staging, 'decrypted')
