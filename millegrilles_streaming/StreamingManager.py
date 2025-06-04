import asyncio

import aiohttp
import logging

from asyncio import TaskGroup
from typing import Callable, Awaitable, Optional

from millegrilles_messages.bus.BusContext import ForceTerminateExecution
from millegrilles_messages.structs.Filehost import Filehost
from millegrilles_streaming.Context import StreamingContext
from millegrilles_streaming.Intake import IntakeHandler, IntakeJob
from millegrilles_streaming.Structs import InformationFuuid, FilehostInvalidException, TooManyRetries


class StreamingManager:

    def __init__(self, context: StreamingContext, intake: IntakeHandler):
        self.__logger = logging.getLogger(__name__+'.'+self.__class__.__name__)
        self.__context = context
        self.__intake = intake
        # Listeners for changes on the filehost
        self.__filehost_listeners: list[Callable[[Optional[Filehost]], Awaitable[None]]] = list()

    @property
    def context(self):
        return self.__context

    async def setup(self):
        # Setup paths
        self.context.download_path.mkdir(parents=True, exist_ok=True)
        self.context.decrypted_path.mkdir(parents=True, exist_ok=True)

    async def run(self):
        try:
            async with TaskGroup() as group:
                group.create_task(self.__reload_filehost_thread())
                group.create_task(self.__intake.run())
        except* Exception:
            if self.__context.stopping is False:
                self.__logger.exception("StreamingManager taskGroup threw exception - quitting")
                self.__context.stop()
                raise ForceTerminateExecution()

    def add_filehost_listener(self, listener: Callable[[Optional[Filehost]], Awaitable[None]]):
        self.__filehost_listeners.append(listener)

    async def __reload_filehost_thread(self):
        while self.__context.stopping is False:
            try:
                await self.reload_filehost_configuration(timeout=15)
                await self.__context.wait(900)
            except asyncio.TimeoutError:
                self.__logger.warning("Timeout loading filehost information, retrying")
                await self.__context.wait(15)
            except:
                self.__logger.exception("Error loading filehost configuration")
                await self.__context.wait(60)

    async def reload_filehost_configuration(self, timeout=3):
        producer = await self.__context.get_producer()
        response = await producer.request(
            dict(), 'CoreTopologie', 'getFilehostForInstance', exchange="1.public", timeout=timeout)

        filehost_response = response.parsed
        filehost_dict = filehost_response['filehost']
        filehost = Filehost.load_from_dict(filehost_dict)
        self.__context.filehost = filehost

        self.__logger.info(f"Using filehost {filehost.filehost_id}, external url: {filehost.url_external}")

        for l in self.__filehost_listeners:
            await l(self.__context.filehost)

    async def add_job(self, file: InformationFuuid) -> IntakeJob:
        CONST_WAIT_RETRY = 1
        CONST_MAX_RETRY = 3
        for i in range(0, CONST_MAX_RETRY):
            try:
                return await self.__intake.add_job(file)
            except FilehostInvalidException:
                if i > 0:  # Throttle retries
                    await self.__context.wait(CONST_WAIT_RETRY)
                try:
                    await self.reload_filehost_configuration()
                except (TimeoutError, asyncio.TimeoutError):
                    self.__logger.info("add_job Timeout reloading filehost information")
                    await self.__context.wait(CONST_WAIT_RETRY)
            except aiohttp.ClientConnectionError as e:
                try:
                    errno = e.errno
                    if errno == -2:
                        self.__logger.error(
                            f"The server (DNS hostname) is unknown, the filehost information is wrong, reloading: {str(e)}")
                        if i > 0:  # Throttle retries
                            await self.__context.wait(CONST_WAIT_RETRY)
                        await self.reload_filehost_configuration()
                        continue
                except AttributeError:
                    pass

                raise e

        raise TooManyRetries('Too many retries')

    async def load_decrypted_information(self, file_information: InformationFuuid):
            return await self.__intake.load_decrypted_information(file_information)
