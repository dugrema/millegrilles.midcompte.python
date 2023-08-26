import asyncio
import datetime
import gzip
import json
import logging
import os
import shutil
import uuid

import pytz

from typing import Optional

from millegrilles_messages.messages import Constantes as ConstantesMillegrilles
from millegrilles_messages.jobs.Intake import IntakeHandler
from millegrilles_messages.MilleGrillesConnecteur import EtatInstance
from millegrilles_messages.messages.MessagesModule import MessageWrapper

from millegrilles_backup import Constantes


class IntakeStreaming(IntakeHandler):
    """
    Gere le dechiffrage des videos.
    """

    def __init__(self, stop_event: asyncio.Event, etat_instance: EtatInstance, timeout_cycle: Optional[int] = None):
        super().__init__(stop_event, etat_instance, timeout_cycle)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

    async def traiter_prochaine_job(self) -> Optional[dict]:
        self.__logger.debug("Traiter job streaming")
        return None

    async def annuler_job(self, job: dict, emettre_evenement=False):
        raise NotImplementedError('must override')
