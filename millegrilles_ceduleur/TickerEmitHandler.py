import asyncio
import logging
import datetime

import pytz

from millegrilles_messages.messages import Constantes
from millegrilles_ceduleur.TickerContext import TickerContext


class TickerEmitHandler:

    def __init__(self, context: TickerContext):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__context = context

    async def run(self):
        await self.__emit_thread()

    async def __emit_thread(self):
        while self.__context.stopping is False:
            # Calculer attente avant la prochaine minute
            attente = determiner_attente()
            await self.__context.wait(attente)
            try:
                await self.__emit_tick()
            except asyncio.CancelledError as e:
                raise e
            except:
                self.__logger.exception("Unhandled error")

    async def __emit_tick(self):
        now = datetime.datetime.now(tz=pytz.UTC)
        date_string = now.isoformat()

        flag_annee = False
        flag_mois = False
        flag_semaine = False
        flag_jour = False

        if now.minute == 0:
            flag_heure = True
            if now.hour == 0:
                flag_jour = True
                if now.day == 0:
                    flag_mois = True
                    if now.month == 0:
                        flag_annee = True
                    if now.weekday() == 0:
                        flag_semaine = True
        else:
            flag_heure = False

        evenement = {
            "date_string": date_string,  # "2023-12-01T14:20:02.930906227+00:00",
            #                               2023-12-01T14:40:01.824390+00:00
            "estampille": int(now.timestamp()),
            "flag_annee": flag_annee,
            "flag_heure": flag_heure,
            "flag_jour": flag_jour,
            "flag_mois": flag_mois,
            "flag_semaine": flag_semaine
        }

        producer = await self.__context.get_producer()

        # Tick
        await producer.event(evenement, domain='ceduleur', action='tick', exchange='1.public')

        # Ping
        await producer.event(evenement, domain='ceduleur', action='ping', exchange='1.public')

        # Ping legacy
        exchanges = [Constantes.SECURITE_PUBLIC, Constantes.SECURITE_PRIVE, Constantes.SECURITE_PROTEGE, Constantes.SECURITE_SECURE]
        for exchange in exchanges:
            await producer.event(evenement, domain='global', action='cedule', exchange=exchange)


def determiner_attente() -> int:
    """ :returns: Retourne le nombre de secondes d'attente avant la prochaine minute """
    now = datetime.datetime.now()

    prochaine_minute = now + datetime.timedelta(minutes=1)
    prochain_temps = datetime.datetime(
        year=prochaine_minute.year,
        month=prochaine_minute.month,
        day=prochaine_minute.day,
        hour=prochaine_minute.hour,
        minute=prochaine_minute.minute,
        second=2
    )
    attente = prochain_temps - now

    return attente.seconds
