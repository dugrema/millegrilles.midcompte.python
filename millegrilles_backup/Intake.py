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

        self.__nom_domaine_attente: Optional[str] = None
        self.__event_attente_fichiers: Optional[asyncio.Event] = None
        self.__notices: Optional[list] = None

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
            self.__nom_domaine_attente = None
            self.__event_attente_fichiers = None
            self.__notices = None

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
            'fin': int(datetime.datetime.utcnow().timestamp()),
            'domaines': self.__domaines,
        }
        if len(self.__notices) > 0:
            evenement['notices'] = self.__notices

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

        # Cleanup repertoire de staging precedent au besoin
        configuration = self._etat_instance.configuration
        dir_backup = configuration.dir_backup

        try:
            shutil.rmtree(os.path.join(dir_backup, 'transactions.3'))
        except FileNotFoundError:
            pass

        for i in range(2, 0, -1):
            path_transactions_prev = os.path.join(dir_backup, 'transactions.%d' % i)
            path_transactions_next = os.path.join(dir_backup, 'transactions.%d' % (i+1))
            try:
                os.rename(path_transactions_prev, path_transactions_next)
            except FileNotFoundError:
                pass

        path_transactions = os.path.join(dir_backup, 'transactions')
        try:
            os.rename(path_transactions, os.path.join(dir_backup, 'transactions.1'))
        except FileNotFoundError:
            pass

        path_staging = os.path.join(dir_backup, 'staging')
        os.rename(path_staging, path_transactions)

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

            # Init flags
            domaine['backup_complete'] = False

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
                self.__notices.append({
                    "domaine": nom_domaine,
                    "erreur": "Erreur recuperation nombre de transactions, le domaine ne repond pas.",
                })

    async def backup_domaine(self, domaine):
        self.__logger.info("Debut backup domaine : %s" % domaine)
        producer = self._etat_instance.producer
        await producer.producer_pret().wait()

        nom_domaine = domaine['domaine']

        # Evenement demarrer backup domaine
        await self.emettre_evenement_maj(domaine)

        await producer.executer_commande(
            {'complet': True},
            nom_domaine,
            Constantes.COMMANDE_DECLENCHER_BACKUP,
            exchange=ConstantesMillegrilles.SECURITE_PRIVE,
            nowait=True
        )

        # Indique que le backup est commence
        domaine['transactions_traitees'] = 0
        domaine['backup_complete'] = False

        await self.emettre_evenement_maj(domaine)

        # Demarrer le backup sur le domaine. Attendre tous les fichiers de backup ou timeout
        self.__nom_domaine_attente = nom_domaine
        self.__event_attente_fichiers.clear()
        pending = [self.__event_attente_fichiers.wait()]
        while self.__event_attente_fichiers.is_set() is False:
            done, pending = await asyncio.wait(pending, timeout=5)
            # Emettre evenement maj backup
            await self.emettre_evenement_maj(domaine)

        domaine['backup_complete'] = True

    async def run_backup(self):
        self.__event_attente_fichiers = asyncio.Event()
        self.__notices = list()

        await self.preparer_backup()

        for domaine in self.__domaines:
            # Verifier que le domaine a precedemment repondu
            if domaine.get('nombre_transactions'):
                await self.backup_domaine(domaine)

        # Effectuer rotation backup (local)
        await self.rotation_backup()

    async def recevoir_evenement(self, message: MessageWrapper):

        contenu = message.parsed
        nom_evenement = contenu['evenement']
        nom_domaine = contenu['domaine']

        domaine_dict = None
        for domaine_info in self.__domaines:
            if domaine_info['domaine'] == nom_domaine:
                domaine_dict = domaine_info
                break

        if nom_evenement == 'backupDemarre':
            pass
        elif nom_evenement == 'cataloguePret':
            if domaine_dict is not None:
                domaine_dict['transactions_traitees'] = contenu['transactions_traitees']
        elif nom_evenement == 'backupTermine':
            if nom_domaine == self.__nom_domaine_attente:
                # Debloquer attente des fichiers pour le domaine
                self.__event_attente_fichiers.set()

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
