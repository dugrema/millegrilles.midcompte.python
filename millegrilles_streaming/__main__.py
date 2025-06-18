import asyncio
import logging
from asyncio import TaskGroup
from concurrent.futures.thread import ThreadPoolExecutor

from typing import Awaitable

from millegrilles_streaming.Configuration import StreamingConfiguration
from millegrilles_streaming.Context import StreamingContext
from millegrilles_messages.bus.BusContext import ForceTerminateExecution, StopListener
from millegrilles_messages.bus.PikaConnector import MilleGrillesPikaConnector
from millegrilles_streaming.Intake import IntakeHandler
from millegrilles_streaming.MgbusHandler import MgbusHandler
from millegrilles_streaming.StagingMaintenance import StagingMaintenanceHandler
from millegrilles_streaming.StreamingManager import StreamingManager
from millegrilles_streaming.WebServer import WebServer

LOGGER = logging.getLogger(__name__)


async def force_terminate_task_group():
    """Used to force termination of a task group."""
    raise ForceTerminateExecution()


async def main():
    config = StreamingConfiguration.load()
    context = StreamingContext(config)

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


async def wiring(context: StreamingContext) -> list[Awaitable]:
    # Some threads get used to handle sync events for the duration of the execution. Ensure there are enough.
    loop = asyncio.get_event_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=10))

    # Create instances
    bus_connector = MilleGrillesPikaConnector(context)
    intake = IntakeHandler(context)
    maintenance_handler = StagingMaintenanceHandler(context)
    manager = StreamingManager(context, intake)

    # Access modules
    web_server = WebServer(manager)
    bus_handler = MgbusHandler(manager)

    # Setup, injecting additional dependencies
    context.bus_connector = bus_connector
    # context.add_reload_listener(intake.clear_session)
    await manager.setup()  # Create folders before rest of setup
    await maintenance_handler.setup()
    await web_server.setup()

    # Create tasks
    coros = [
        context.run(),
        maintenance_handler.run(),
        manager.run(),
        web_server.run(),
        bus_handler.run(),
    ]

    return coros


if __name__ == '__main__':
    asyncio.run(main())
    LOGGER.info("Stopped")
