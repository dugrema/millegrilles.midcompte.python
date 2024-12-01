import logging
from asyncio import TaskGroup
from typing import Callable, Awaitable, Optional

from millegrilles_messages.bus.BusContext import ForceTerminateExecution
from millegrilles_messages.structs.Filehost import Filehost
from millegrilles_streaming.Context import StreamingContext
from millegrilles_streaming.Intake import IntakeHandler, IntakeJob
from millegrilles_streaming.Structs import InformationFuuid


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
                group.create_task(self.__staging_cleanup())
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
                await self.reload_filehost_configuration()
                await self.__context.wait(900)
            except:
                self.__logger.exception("Error loading filehost configuration")
                await self.__context.wait(30)

    async def reload_filehost_configuration(self):
        producer = await self.__context.get_producer()
        response = await producer.request(
            dict(), 'CoreTopologie', 'getFilehostForInstance', exchange="1.public")

        try:
            filehost_response = response.parsed
            filehost_dict = filehost_response['filehost']
            filehost = Filehost.load_from_dict(filehost_dict)
            self.__context.filehost = filehost
        except:
            self.__logger.exception("Error loading filehost")
            self.__context.filehost = None

        for l in self.__filehost_listeners:
            await l(self.__context.filehost)

    async def __staging_cleanup(self):
        while self.__context.stopping is False:
            # TODO - cleanup

            await self.__context.wait(300)

    async def add_job(self, file: InformationFuuid) -> IntakeJob:
        return await self.__intake.add_job(file)

    async def load_decrypted_information(self, file_information: InformationFuuid):
        return await self.__intake.load_decrypted_information(file_information)
