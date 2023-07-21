import asyncio
import datetime
import logging
import pytz

from typing import Optional

from millegrilles_messages.messages import Constantes as ConstantesMillegrilles
from millegrilles_messages.jobs.Intake import IntakeHandler
from millegrilles_messages.MilleGrillesConnecteur import EtatInstance

from millegrilles_backup import Constantes

class IntakeBackup(IntakeHandler):
    """
    Gere l'execution d'un backup complet.
    Recoit aussi les backup incrementaux (aucune orchestration requise).
    """

    def __init__(self, stop_event: asyncio.Event, etat_instance: EtatInstance, timeout_cycle: Optional[int] = None):
        super().__init__(stop_event, etat_instance, timeout_cycle)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

    async def traiter_prochaine_job(self) -> Optional[dict]:
        self.__logger.debug("Traiter job backup")

        debut_backup = datetime.datetime.now(tz=pytz.utc)
        try:
            await self.emettre_evenement_demarrer(debut_backup)
            domaines = await self.charger_liste_domaines(debut_backup)
            await self.run_backup(debut_backup, domaines)
        except Exception as e:
            self.__logger.exception("Erreur execution backup")
            # Emettre evenement echec
            await self.emettre_evenement_echec(debut_backup, e)
        else:
            # Emettre evenement succes
            await self.emettre_evenement_succes(debut_backup)

        return None

    async def annuler_job(self, job: dict, emettre_evenement=False):
        raise NotImplementedError('must override')

    async def emettre_evenement_demarrer(self, date_debut: datetime.datetime):
        self.__logger.info("Evenement demarrer backup")
        producer = self._etat_instance.producer
        await producer.producer_pret().wait()
        evenement = {
            'debut': int(date_debut.timestamp()),
        }

        await producer.emettre_evenement(
            evenement,
            ConstantesMillegrilles.DOMAINE_BACKUP,
            Constantes.EVENEMENT_BACKUP_DEMARRAGE,
            exchanges=ConstantesMillegrilles.SECURITE_PRIVE
        )

    async def emettre_evenement_maj(self, debut: datetime.datetime, domaines: list):
        self.__logger.info("Evenement maj")
        producer = self._etat_instance.producer
        await producer.producer_pret().wait()

        domaines_noms = [d['domaine'] for d in domaines]

        evenement = {
            'debut': int(debut.timestamp()),
            'domaines': domaines_noms,
        }

        await producer.emettre_evenement(
            evenement,
            ConstantesMillegrilles.DOMAINE_BACKUP,
            Constantes.EVENEMENT_BACKUP_MISEAJOUR,
            exchanges=ConstantesMillegrilles.SECURITE_PRIVE
        )

    async def emettre_evenement_succes(self, date_debut: datetime.datetime):
        self.__logger.info("Evenement succes backup")
        producer = self._etat_instance.producer
        await producer.producer_pret().wait()
        evenement = {
            'debut': int(date_debut.timestamp()),
        }

        await producer.emettre_evenement(
            evenement,
            ConstantesMillegrilles.DOMAINE_BACKUP,
            Constantes.EVENEMENT_BACKUP_SUCCES,
            exchanges=ConstantesMillegrilles.SECURITE_PRIVE
        )

    async def emettre_evenement_echec(self, date_debut: datetime.datetime, e):
        self.__logger.info("Evenement echec backup : %s", e)
        producer = self._etat_instance.producer
        await producer.producer_pret().wait()
        evenement = {
            'debut': int(date_debut.timestamp()),
            'ok': False,
            'err': '%s' % e
        }

        await producer.emettre_evenement(
            evenement,
            ConstantesMillegrilles.DOMAINE_BACKUP,
            Constantes.EVENEMENT_BACKUP_ECHEC,
            exchanges=ConstantesMillegrilles.SECURITE_PRIVE
        )

    async def charger_liste_domaines(self, date_debut: datetime.datetime) -> list:
        producer = self._etat_instance.producer
        await producer.producer_pret().wait()

        # Charger domaines a partir de topologie
        reponse = await producer.executer_requete(
            dict(),
            ConstantesMillegrilles.DOMAINE_CORE_TOPOLOGIE,
            ConstantesMillegrilles.REQUETE_CORETOPOLOGIE_LISTE_DOMAINES,
            exchange=ConstantesMillegrilles.SECURITE_PRIVE)

        if reponse.parsed['ok'] is not True:
            raise Exception('Erreur reception liste domaines de CoreTopologie')

        self.__logger.debug("Reponse domaines : %s" % reponse)
        domaines = reponse.parsed['resultats']

        # Emettre liste domaines pour backup
        await self.emettre_evenement_maj(date_debut, domaines)

        return domaines

    async def rotation_backup(self):
        self.__logger.warning("TODO - rotation repertoire backup")

    async def preparer_backup(self, domaines: list):
        producer = self._etat_instance.producer
        await producer.producer_pret().wait()

        # Interroger chaque domaine pour obtenir nombre de transactions
        for domaine in domaines:
            nom_domaine = domaine['domaine']
            self.__logger.info("Recuperer nombre transactions du domaine %s" % nom_domaine)
            try:
                reponse = await producer.executer_requete(
                    dict(),
                    nom_domaine,
                    ConstantesMillegrilles.REQUETE_GLOBAL_NOMBRE_TRANSACTIONS,
                    exchange=ConstantesMillegrilles.SECURITE_PRIVE
                )
                if reponse.parsed['ok'] is not True:
                    self.__logger.info("Erreur recuperation nombre transactions pour domaine %s, SKIP" % nom_domaine)

                domaine['nombre_transactions'] = reponse.parsed['nombre_transactions']
                self.__logger.info("Information domaine : %s" % domaine)
            except:
                self.__logger.exception("Erreur recuperation nombre transactions pour domaine %s, SKIP" % nom_domaine)

        # Effectuer rotation backup (local)
        await self.rotation_backup()

        # Emettre commande de reset de backup (global)


        pass

    async def run_backup(self, date_debut, domaines):
        await self.preparer_backup(domaines)
