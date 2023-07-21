import asyncio
import datetime
import logging
import lzma
import os
import shutil

import pytz

from typing import Optional

from millegrilles_messages.messages import Constantes as ConstantesMillegrilles
from millegrilles_messages.jobs.Intake import IntakeHandler
from millegrilles_messages.MilleGrillesConnecteur import EtatInstance
from millegrilles_messages.messages.MessagesModule import MessageWrapper

from millegrilles_backup import Constantes


class IntakeBackup(IntakeHandler):
    """
    Gere l'execution d'un backup complet.
    Recoit aussi les backup incrementaux (aucune orchestration requise).
    """

    def __init__(self, stop_event: asyncio.Event, etat_instance: EtatInstance, timeout_cycle: Optional[int] = None):
        super().__init__(stop_event, etat_instance, timeout_cycle)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

        # Variables partagees par une meme job
        self.__debut_backup: Optional[datetime.datetime] = None
        self.__domaines: Optional[list] = None
        self.__event_attente_fichiers: Optional[asyncio.Event] = None

    async def traiter_prochaine_job(self) -> Optional[dict]:
        self.__logger.debug("Traiter job backup")

        self.__debut_backup = datetime.datetime.now(tz=pytz.utc)
        try:
            await self.emettre_evenement_demarrer()
            await self.charger_liste_domaines()
            await self.run_backup()
        except Exception as e:
            self.__logger.exception("Erreur execution backup")
            # Emettre evenement echec
            await self.emettre_evenement_echec(e)
        else:
            # Emettre evenement succes
            await self.emettre_evenement_succes()
        finally:
            # Cleanup job
            self.__debut_backup = None
            self.__domaines = None
            self.__event_attente_fichiers = None

        return None

    async def annuler_job(self, job: dict, emettre_evenement=False):
        raise NotImplementedError('must override')

    async def emettre_evenement_demarrer(self):
        self.__logger.info("Evenement demarrer backup")
        producer = self._etat_instance.producer
        await producer.producer_pret().wait()
        evenement = {
            'debut': int(self.__debut_backup.timestamp()),
        }

        await producer.emettre_evenement(
            evenement,
            ConstantesMillegrilles.DOMAINE_BACKUP,
            Constantes.EVENEMENT_BACKUP_DEMARRAGE,
            exchanges=ConstantesMillegrilles.SECURITE_PRIVE
        )

    async def emettre_evenement_maj(self, domaine_courant: Optional[dict] = None):
        self.__logger.info("Evenement maj")
        producer = self._etat_instance.producer
        await producer.producer_pret().wait()

        evenement = {
            'debut': int(self.__debut_backup.timestamp()),
            'domaines': self.__domaines,
        }

        if domaine_courant is not None:
            evenement['domaine'] = domaine_courant

        await producer.emettre_evenement(
            evenement,
            ConstantesMillegrilles.DOMAINE_BACKUP,
            Constantes.EVENEMENT_BACKUP_MISEAJOUR,
            exchanges=ConstantesMillegrilles.SECURITE_PRIVE
        )

    async def emettre_evenement_succes(self):
        self.__logger.info("Evenement succes backup")
        producer = self._etat_instance.producer
        await producer.producer_pret().wait()
        evenement = {
            'debut': int(self.__debut_backup.timestamp()),
        }

        await producer.emettre_evenement(
            evenement,
            ConstantesMillegrilles.DOMAINE_BACKUP,
            Constantes.EVENEMENT_BACKUP_SUCCES,
            exchanges=ConstantesMillegrilles.SECURITE_PRIVE
        )

    async def emettre_evenement_echec(self, e):
        self.__logger.info("Evenement echec backup : %s", e)
        producer = self._etat_instance.producer
        await producer.producer_pret().wait()
        evenement = {
            'debut': int(self.__debut_backup.timestamp()),
            'ok': False,
            'err': '%s' % e
        }

        await producer.emettre_evenement(
            evenement,
            ConstantesMillegrilles.DOMAINE_BACKUP,
            Constantes.EVENEMENT_BACKUP_ECHEC,
            exchanges=ConstantesMillegrilles.SECURITE_PRIVE
        )

    async def charger_liste_domaines(self) -> list:
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
        self.__domaines = reponse.parsed['resultats']

        # Emettre liste domaines pour backup
        await self.emettre_evenement_maj(self.__domaines)

        return self.__domaines

    async def rotation_backup(self):
        self.__logger.warning("TODO - rotation repertoire backup")

    async def preparer_backup(self):
        producer = self._etat_instance.producer
        await producer.producer_pret().wait()

        # Cleanup repertoire de staging precedent au besoin
        configuration = self._etat_instance.configuration
        dir_backup = configuration.dir_backup
        path_domaine = os.path.join(dir_backup, 'staging')
        try:
            shutil.rmtree(path_domaine)
        except FileNotFoundError:
            pass  # OK

        # Interroger chaque domaine pour obtenir nombre de transactions
        for domaine in self.__domaines:
            nom_domaine = domaine['domaine']
            self.__logger.info("Recuperer nombre transactions du domaine %s" % nom_domaine)
            try:
                reponse = await producer.executer_requete(
                    dict(),
                    nom_domaine,
                    ConstantesMillegrilles.REQUETE_GLOBAL_NOMBRE_TRANSACTIONS,
                    exchange=ConstantesMillegrilles.SECURITE_PRIVE,
                    timeout=5
                )
                if reponse.parsed['ok'] is not True:
                    self.__logger.info("Erreur recuperation nombre transactions pour domaine %s, SKIP" % nom_domaine)

                domaine['nombre_transactions'] = reponse.parsed['nombre_transactions']
                self.__logger.info("Information domaine : %s" % domaine)
            except:
                self.__logger.exception("Erreur recuperation nombre transactions pour domaine %s, SKIP" % nom_domaine)

        # Effectuer rotation backup (local)
        await self.rotation_backup()

    async def backup_domaine(self, domaine):
        self.__logger.info("Debut backup domaine : %s" % domaine)
        producer = self._etat_instance.producer
        await producer.producer_pret().wait()

        nom_domaine = domaine['domaine']

        # Evenement demarrer backup domaine
        await self.emettre_evenement_maj(domaine)

        # # Reset le flag de backup de toutes les transactions du domaine
        # reponse_reset = await producer.executer_commande(dict(), nom_domaine, Constantes.COMMANDE_RESET_BACKUP)
        #
        # if reponse_reset.parsed['ok'] is not True:
        #     raise Exception("Erreur reset transactions domaine %s" % nom_domaine)

        await producer.executer_commande(
            {'complet': True},
            nom_domaine,
            Constantes.COMMANDE_DECLENCHER_BACKUP,
            exchange=ConstantesMillegrilles.SECURITE_PRIVE,
            nowait=True
        )

        # Indique que le backup est commence
        domaine['transactions_traitees'] = 0
        await self.emettre_evenement_maj(domaine)

        # Demarrer le backup sur le domaine. Attendre tous les fichiers de backup ou timeout
        # TODO : fix me, utiliser reset du timeout
        wait_coro = self.__event_attente_fichiers.wait()
        await asyncio.wait([wait_coro], timeout=5)
        wait_coro.close()

    async def run_backup(self):
        await self.preparer_backup()

        self.__event_attente_fichiers = asyncio.Event()

        for domaine in self.__domaines:
            # Verifier que le domaine a precedemment repondu
            if domaine.get('nombre_transactions'):
                await self.backup_domaine(domaine)

    async def recevoir_evenement(self, message: MessageWrapper):
        pass

    async def recevoir_fichier_transactions(self, message: MessageWrapper):
        contenu = message.parsed

        self.__logger.debug("Recu fichier transactions : %s" % contenu)

        nombre_transactions = contenu['nombre_transactions']
        original = message.contenu
        nom_domaine = contenu['domaine']
        nom_fichier: str = contenu['catalogue_nomfichier']
        hachage = contenu['data_hachage_bytes']

        nom_fichier_split = nom_fichier.split('.')
        nom_fichier_split.insert(1, hachage[-8:])
        nom_fichier = '.'.join(nom_fichier_split)

        # Sauvegarder original sur disque
        configuration = self._etat_instance.configuration
        dir_backup = configuration.dir_backup

        if self.__domaines is None:
            # Backup incremental, mettre sous repertoire transactions
            path_domaine = os.path.join(dir_backup, 'transactions', nom_domaine)
        else:
            # Backup complet, utiliser staging
            path_domaine = os.path.join(dir_backup, 'staging', nom_domaine)

        os.makedirs(path_domaine, exist_ok=True)
        path_fichier = os.path.join(path_domaine, nom_fichier)
        with lzma.open(path_fichier, 'wb') as fichier:
            fichier.write(original)

        if self.__domaines is not None:
            # Statistiques cumulatives pour le backup complet du domaine
            for domaine in self.__domaines:
                if domaine['domaine'] == nom_domaine:
                    try:
                        domaine['fichiers'] += 1
                    except KeyError:
                        domaine['fichiers'] = 1
                    try:
                        domaine['transactions_sauvegardees'] += nombre_transactions
                    except KeyError:
                        domaine['transactions_sauvegardees'] = nombre_transactions

                    break

            # Maj des stats via MQ
            await self.emettre_evenement_maj(None)

        else:
            # Backup incremental
            pass

        return {'ok': True}
