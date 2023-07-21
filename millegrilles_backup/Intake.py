import asyncio
import logging

from typing import Optional

from millegrilles_messages.jobs.Intake import IntakeHandler
from millegrilles_messages.MilleGrillesConnecteur import EtatInstance


class IntakeBackup(IntakeHandler):

    def __init__(self, stop_event: asyncio.Event, etat_instance: EtatInstance, timeout_cycle: Optional[int] = None):
        super().__init__(stop_event, etat_instance, timeout_cycle)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

    async def traiter_prochaine_job(self) -> dict:
        self.__logger.debug("Traiter job backup")
        return None

    async def annuler_job(self, job: dict, emettre_evenement=False):
        raise NotImplementedError('must override')
