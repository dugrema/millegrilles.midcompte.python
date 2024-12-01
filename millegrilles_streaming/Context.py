import aiohttp
import logging
import pathlib
import ssl

from typing import Optional, Callable, Awaitable
from urllib.parse import urlparse

from millegrilles_messages.bus.BusConfiguration import MilleGrillesBusConfiguration
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

        self.__filehost: Optional[Filehost] = None
        self.__filehost_url: Optional[str] = None
        self.__tls_method: Optional[str] = None
        self.__ssl_context_filehost: Optional[ssl.SSLContext] = None

    def reload(self):
        super().reload()
        for listener in self.__reload_listeners:
            listener()

    def add_reload_listener(self, listener: Callable[[], None]):
        self.__reload_listeners.append(listener)

    @property
    def configuration(self) -> StreamingConfiguration:
        return super().configuration

    @property
    def bus_connector(self):
        return self.__bus_connector

    @bus_connector.setter
    def bus_connector(self, value: MilleGrillesPikaConnector):
        self.__bus_connector = value

    async def get_producer(self):
        return await self.__bus_connector.get_producer()

    @property
    def filehost(self) -> Optional[Filehost]:
        return self.__filehost

    @filehost.setter
    def filehost(self, value: Filehost):
        self.__filehost = value

        # Pick URL
        url, tls_method = StreamingContext.__load_url(value)
        self.__filehost_url = url.geturl()
        self.__tls_method = tls_method

        # Configure ssl context for the filehost

    @property
    def filehost_url(self):
        return self.__filehost_url

    @property
    def tls_method(self):
        return self.__tls_method

    # @property
    # def ssl_context_filehost(self):
    #     return self.__ssl_context_filehost

    def get_tcp_connector(self) -> aiohttp.TCPConnector:
        # Prepare connection information (SSL)
        ssl_context = None
        verify = True
        if self.__tls_method == 'millegrille':
            ssl_context = self.ssl_context
        elif self.__tls_method == 'nocheck':
            verify = False

        connector = aiohttp.TCPConnector(ssl=ssl_context, verify_ssl=verify)

        return connector

    def get_http_session(self, timeout: Optional[aiohttp.ClientTimeout] = None) -> aiohttp.ClientSession:
        connector = self.get_tcp_connector()
        return aiohttp.ClientSession(timeout=timeout, connector=connector)

    @property
    def download_path(self) -> pathlib.Path:
        return pathlib.Path(self.configuration.dir_staging, 'download')

    @property
    def decrypted_path(self) -> pathlib.Path:
        return pathlib.Path(self.configuration.dir_staging, 'decrypted')

    @staticmethod
    def __load_url(filehost: Filehost):
        if filehost.url_external:
            url = urlparse(filehost.url_external)
            tls_method = filehost.tls_external
        elif filehost.url_internal:
            url = urlparse(filehost.url_internal)
            tls_method = 'millegrille'
        else:
            raise ValueError("No valid URL")
        return url, tls_method

