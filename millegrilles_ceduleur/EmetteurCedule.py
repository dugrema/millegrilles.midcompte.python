import asyncio
import logging
import datetime

import pytz


class Ceduleur:

    def __init__(self, etat, stop_event: asyncio.Event):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__etat = etat
        self.__stop_event = stop_event

    async def run(self):
        while self.__stop_event.is_set() is False:
            # Calculer attente avant la prochaine minute
            attente = determiner_attente()

            try:
                await asyncio.wait_for(self.__stop_event.wait(), attente)
                return  # Stopped
            except asyncio.TimeoutError:
                pass

            try:
                await self.emettre_ping()
            except:
                self.__logger.exception("run Erreur emettre_ping")

    async def emettre_ping(self):
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

        producer = self.__etat.producer

        # Nouvelle approche
        await producer.emettre_evenement(evenement,
                                         domaine='ceduleur', action='ping',
                                         exchanges='1.public')

        # Ping legacy
        await producer.emettre_evenement(evenement,
                                         domaine='global', action='cedule',
                                         exchanges=['1.public', '2.prive', '3.protege', '4.secure'])


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
