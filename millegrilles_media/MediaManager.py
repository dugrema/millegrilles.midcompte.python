import logging
from asyncio import TaskGroup
from typing import Callable, Awaitable, Optional

from millegrilles_media.Context import MediaContext
from millegrilles_media.intake import IntakeJobVideo, IntakeJobImage
from millegrilles_messages.structs.Filehost import Filehost


class MediaManager:

    def __init__(self, context: MediaContext, intake_images: IntakeJobImage, intake_videos: IntakeJobVideo):
        self.__logger = logging.getLogger(__name__+'.'+self.__class__.__name__)
        self.__context = context
        self.__intake_images = intake_images
        self.__intake_videos = intake_videos
        # Listeners for changes on the filehost
        self.__filehost_listeners: list[Callable[[Optional[Filehost]], Awaitable[None]]] = list()

    async def setup(self):
        # Create staging folders
        self.__context.dir_media_staging.mkdir(parents=True, exist_ok=True)

    async def run(self):
        async with TaskGroup() as group:
            group.create_task(self.__reload_filehost_thread())
            group.create_task(self.__staging_cleanup())

    async def process_image_job(self, job: dict):
        await self.__intake_images.process_job(job)

    async def process_video_job(self, job: dict):
        await self.__intake_videos.process_job(job)

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
