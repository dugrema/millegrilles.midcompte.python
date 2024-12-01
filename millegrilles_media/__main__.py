import asyncio
import logging
from asyncio import TaskGroup
from concurrent.futures.thread import ThreadPoolExecutor

from typing import Awaitable

from millegrilles_media.Configuration import ConfigurationMedia
from millegrilles_media.Context import MediaContext
from millegrilles_media.CommandHandler import CommandHandler
from millegrilles_media.MediaManager import MediaManager
from millegrilles_media.intake import IntakeJobImage, IntakeJobVideo
from millegrilles_messages.bus.BusContext import ForceTerminateExecution, StopListener
from millegrilles_messages.bus.PikaConnector import MilleGrillesPikaConnector

LOGGER = logging.getLogger(__name__)


async def force_terminate_task_group():
    """Used to force termination of a task group."""
    raise ForceTerminateExecution()


async def main():
    config = ConfigurationMedia.load()
    context = MediaContext(config)

    LOGGER.setLevel(logging.INFO)
    LOGGER.info("Starting")

    # Wire classes together, gets awaitables to run
    coros = await wiring(context)

    try:
        # Use taskgroup to run all threads
        async with TaskGroup() as group:
            for coro in coros:
                group.create_task(coro)

            # Create a listener that fires a task to cancel all other tasks
            async def stop_group():
                group.create_task(force_terminate_task_group())
            stop_listener = StopListener(stop_group)
            context.register_stop_listener(stop_listener)

    except* (ForceTerminateExecution, asyncio.CancelledError):
        pass  # Result of the termination task


async def wiring(context: MediaContext) -> list[Awaitable]:
    # Some threads get used to handle sync events for the duration of the execution. Ensure there are enough.
    loop = asyncio.get_event_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=10))

    # Create instances
    bus_connector = MilleGrillesPikaConnector(context)
    context.bus_connector = bus_connector
    intake_images = IntakeJobImage(context)
    intake_videos = IntakeJobVideo(context)

    manager = MediaManager(context, intake_images, intake_videos)

    command_handler = CommandHandler(context, manager)
    manager.add_filehost_listener(command_handler.on_filehosting_update)
    await manager.setup()  # Create folders for other modules
    await command_handler.setup()

    # Create tasks
    coros = [
        context.run(),
        bus_connector.run(),
        manager.run(),
        command_handler.run(),
    ]

    return coros


if __name__ == '__main__':
    asyncio.run(main())
    LOGGER.info("Stopped")
