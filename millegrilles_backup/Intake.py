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


class IntakeBackup(IntakeHandler):
    """
    Gere l'execution d'un backup complet.
    Recoit aussi les backup incrementaux (aucune orchestration requise).
    """

    def __init__(self, stop_event: asyncio.Event, etat_instance: EtatInstance, timeout_cycle: Optional[int] = None, triggers_complete: Optional[list] = None):
        super().__init__(stop_event, etat_instance, timeout_cycle)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

        self.__triggers_completes = triggers_complete

        # Variables partagees par une meme job
        self.__debut_backup: Optional[datetime.datetime] = None
        self.__info_backup: Optional[dict] = None
        self.__domaines: Optional[list] = None

        self.__nom_domaine_attente: Optional[str] = None
        self.__dernier_evenement_domaine: Optional[datetime.datetime] = None
        self.__event_attente_fichiers: Optional[asyncio.Event] = None
        self.__notices: Optional[list] = None
        self.__compteur_fichiers_domaine: Optional[int] = None

    async def trigger_backup_complete(self):
        if self.__triggers_completes is not None:
            for trigger in self.__triggers_completes:
                await trigger()

    async def traiter_prochaine_job(self) -> Optional[dict]:
        self.__logger.debug("Traiter job backup")

        self.__debut_backup = datetime.datetime.utcnow()
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
            self.__info_backup = None
            self.__domaines = None
            self.__nom_domaine_attente = None
            self.__event_attente_fichiers = None
            self.__notices = None
            self.__dernier_evenement_domaine = None
            self.__compteur_fichiers_domaine = None

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
        self.__logger.debug("rotation repertoire backup")

        # Cleanup repertoire de staging precedent au besoin
        configuration = self._etat_instance.configuration
        dir_backup = configuration.dir_backup

        path_transactions = os.path.join(dir_backup, 'transactions')
        try:
            path_fichier_data = os.path.join(path_transactions, Constantes.FICHIER_BACKUP_COURANT)
            with open(path_fichier_data, 'r') as fichier:
                info_backup = json.load(fichier)
            uuid_backup = info_backup['uuid_backup']
            path_destination = os.path.join(dir_backup, uuid_backup)
            os.rename(path_transactions, os.path.join(dir_backup, path_destination))
        except KeyError:
            self.__logger.debug("Backup set %s invalide, on le supprime", path_transactions)
            shutil.rmtree(path_transactions)
        except FileNotFoundError:
            pass

        # Renommer staging vers transactions (backup courant)
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
        os.makedirs(path_domaine)

        # Creer nouveau fichier d'information de backup avec uuid unique
        self.__info_backup = {
            'uuid_backup': str(uuid.uuid4()),
            'date': int(datetime.datetime.utcnow().timestamp()),
            'en_cours': True,
        }
        with open(os.path.join(path_domaine, Constantes.FICHIER_BACKUP_COURANT), 'w') as fichier:
            json.dump(self.__info_backup, fichier)

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
                    timeout=30
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

        self.__info_backup['domaines'] = self.__domaines
        with open(os.path.join(path_domaine, Constantes.FICHIER_BACKUP_COURANT), 'w') as fichier:
            json.dump(self.__info_backup, fichier)

    async def completer_backup(self):
        configuration = self._etat_instance.configuration
        dir_backup = configuration.dir_backup
        path_domaine = os.path.join(dir_backup, 'staging')

        self.__info_backup['en_cours'] = False

        with open(os.path.join(path_domaine, Constantes.FICHIER_BACKUP_COURANT), 'w') as fichier:
            json.dump(self.__info_backup, fichier)

        await self.trigger_backup_complete()

    async def backup_domaine(self, domaine):
        self.__logger.info("Debut backup domaine : %s" % domaine)
        producer = self._etat_instance.producer
        await producer.producer_pret().wait()

        nom_domaine = domaine['domaine']

        # Evenement demarrer backup domaine
        await self.emettre_evenement_maj(domaine)

        # Clear flag et emet commande de backup du domaine
        self.__event_attente_fichiers.clear()

        demarrage_ok = False
        for i in range(1, 4):
            try:
                reponse = await producer.executer_commande(
                    {'complet': True},
                    nom_domaine,
                    Constantes.COMMANDE_DECLENCHER_BACKUP,
                    exchange=ConstantesMillegrilles.SECURITE_PRIVE,
                    timeout=20
                )
                demarrage_ok = reponse.parsed.get('ok') or False
                if demarrage_ok is True:
                    break
                elif demarrage_ok is False and reponse.parsed.get('code') == 2:
                    # Le backup vient juste d'etre execute. On attend 30 secondes
                    await asyncio.sleep(30)
            except asyncio.TimeoutError:
                self.__logger.exception("Erreur attente debut backup domaine %s (try %d de 3)" % (nom_domaine, i))

        if demarrage_ok is False:
            self.__logger.warning("Le domaine %s n'est pas disponible pour le backup" % nom_domaine)
            raise Exception("Echec backup domaine %s, timeout demarrage" % nom_domaine)

        # Indique que le backup est commence
        domaine['transactions_traitees'] = 0
        domaine['backup_complete'] = False

        await self.emettre_evenement_maj(domaine)

        # Demarrer le backup sur le domaine. Attendre tous les fichiers de backup ou timeout
        self.__nom_domaine_attente = nom_domaine
        self.__dernier_evenement_domaine = datetime.datetime.utcnow()
        self.__compteur_fichiers_domaine = 0

        pending = [self.__event_attente_fichiers.wait()]
        while self.__event_attente_fichiers.is_set() is False:
            # Verifier si on a un timeout pour ce domaine
            expiration = datetime.datetime.utcnow() - datetime.timedelta(seconds=90)
            if expiration > self.__dernier_evenement_domaine:
                raise Exception("Echec backup domaine %s, timeout catalogues" % nom_domaine)

            # Attendre et emettre evenement maj backup
            done, pending = await asyncio.wait(pending, timeout=5)
            await self.emettre_evenement_maj(domaine)

        # Succes pour le domaine courant
        domaine['backup_complete'] = True

        # Reset etat pour prochain domaine
        self.__event_attente_fichiers.clear()
        self.__compteur_fichiers_domaine = None

    async def run_backup(self):
        if self._etat_instance.backup_inhibe is True:
            self.__logger.warning("Backup inhibe (e.g. restauration en cours) - ABORT")
            return

        self.__event_attente_fichiers = asyncio.Event()
        self.__notices = list()

        await self.preparer_backup()

        for domaine in self.__domaines:
            # Verifier que le domaine a precedemment repondu
            if domaine.get('nombre_transactions'):
                try:
                    await self.backup_domaine(domaine)
                except Exception as e:
                    self.__logger.exception("Erreur traitement domaine %s" % domaine['domaine'])
                    self.__notices.append({
                        'domaine': domaine['domaine'],
                        'erreur': "%s" % e
                    })

        await self.completer_backup()

        # Effectuer rotation backup (local)
        await self.rotation_backup()

    async def recevoir_evenement(self, message: MessageWrapper):
        if self.__domaines is None:
            return  # Backup n'est pas en cours

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
            if self.__nom_domaine_attente == nom_domaine:
                self.__dernier_evenement_domaine = datetime.datetime.utcnow()
        elif nom_evenement == 'backupTermine':
            if self.__nom_domaine_attente == nom_domaine:
                # Debloquer attente des fichiers pour le domaine
                self.__dernier_evenement_domaine = datetime.datetime.utcnow()
                self.__event_attente_fichiers.set()

        pass

    async def recevoir_fichier_transactions(self, message: MessageWrapper):
        contenu = message.parsed

        self.__logger.debug("Recu fichier transactions : %s" % contenu)

        nombre_transactions = contenu['nombre_transactions']
        original = message.contenu
        nom_domaine = contenu['domaine']
        hachage = contenu['data_hachage_bytes']
        date_transaction_debut = datetime.datetime.fromtimestamp(contenu['date_transactions_debut'])
        date_debut_format = date_transaction_debut.strftime("%Y%m%dT%H%M%SZ")

        if self.__compteur_fichiers_domaine is not None:
            # On est dans un backup complet, utiliser compteur de 8 chiffres (00000000, 00000001, ...)
            id_fichier = "C{:0>8}".format(self.__compteur_fichiers_domaine)
            self.__compteur_fichiers_domaine += 1  # Incrementer compteur
        else:
            # Mode incremental, utiliser fin du hachage de contenu
            id_fichier = 'I%s' % hachage[-8:]
        nom_fichier = '_'.join([nom_domaine, date_debut_format, id_fichier])

        nom_fichier += '.json.gz'

        if self.__nom_domaine_attente == nom_domaine:
            self.__dernier_evenement_domaine = datetime.datetime.utcnow()

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
        with gzip.open(path_fichier, 'wb') as fichier:
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
            await self.trigger_backup_complete()

        return {'ok': True}
