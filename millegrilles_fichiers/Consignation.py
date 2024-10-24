import datetime
import pathlib
import sqlite3
import pytz

import aiohttp
import asyncio
import logging
import tempfile

from aiohttp import web
from contextlib import asynccontextmanager
from typing import Optional

from millegrilles_messages.messages import Constantes as ConstantesMillegrilles
from millegrilles_messages.MilleGrillesConnecteur import MilleGrillesConnecteur
from millegrilles_messages.messages.MessagesModule import MessageWrapper
from millegrilles_messages.messages.MessagesModule import MessageProducerFormatteur

from millegrilles_fichiers import Constantes
from millegrilles_fichiers.EtatFichiers import EtatFichiers
from millegrilles_fichiers.ConsignationStore import ConsignationStore, map_type
from millegrilles_fichiers.Synchronisation import SyncManager
from millegrilles_fichiers.SQLiteDao import SQLiteConnection, SQLiteDetachedVisiteAppend
from millegrilles_fichiers.ConsignationBackup import ConsignationBackup


class StoreNonInitialise(Exception):
    pass


class InformationFuuid:

    def __init__(self, fuuid: str):
        self.fuuid = fuuid
        self.taille: Optional[int] = None               # Taille du fichier
        self.path_complet: Optional[str] = None         # Path complet sur disque du fichier dechiffre

    @staticmethod
    def resolve_fuuid(etat_fichiers: EtatFichiers, fuuid: str):
        """ Trouver le path local du fichier par son fuuid. """
        info = InformationFuuid(fuuid)
        return info


class ConsignationHandler:
    """
    Download et dechiffre les fichiers de media a partir d'un serveur de consignation
    """

    def __init__(self, stop_event: asyncio.Event, etat_instance: EtatFichiers):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__stop_event = stop_event
        self.__etat_instance = etat_instance

        self.__sync_manager = SyncManager(self)

        self.__store_consignation: Optional[ConsignationStore] = None
        self.__store_backup = ConsignationBackup(stop_event, etat_instance, self)

        self.__url_consignation_primaire: Optional[str] = None

        self.__session_http_download: Optional[aiohttp.ClientSession] = None
        self.__session_http_requests: Optional[aiohttp.ClientSession] = None

        # self.__est_primaire: Optional[bool] = None
        self.__store_pret_event: Optional[asyncio.Event] = None
        self.__traiter_cedule_event: Optional[asyncio.Event] = None
        self.__message_cedule: Optional[MessageWrapper] = None

        self.__timestamp_dernier_sync: Optional[datetime.datetime] = None
        self.__timestamp_visite: Optional[datetime.datetime] = None
        self.__timestamp_verification: Optional[datetime.datetime] = None
        self.__timestamp_orphelins: Optional[datetime.datetime] = None

        self.__intervalle_visites = datetime.timedelta(seconds=Constantes.CONST_INTERVALLE_VISITE_MILLEGRILLE)
        self.__intervalle_verification = datetime.timedelta(seconds=Constantes.CONST_INTERVALLE_VERIFICATION)
        self.__intervalle_orphelins = datetime.timedelta(seconds=Constantes.CONST_INTERVALLE_ORPHELINS)

        self.__rabbitmq_dao: Optional[MilleGrillesConnecteur] = None

    async def store_pret_wait(self):
        await self.__store_pret_event.wait()

    async def run(self):
        self.__logger.info("Demarrage run")

        self.__store_pret_event = asyncio.Event()

        await asyncio.gather(
            self.entretien(),
            self.entretien_store(),
            self.thread_emettre_etat(),
            self.__sync_manager.run(),
            self.__store_backup.run(),
            self.thread_traiter_cedule(),
            self.thread_sync_backups_v2_secondaire(),
        )

        self.__logger.info("Fin run")

    async def entretien(self):

        await asyncio.wait_for(self.__etat_instance.producer_wait(), timeout=20)

        while self.__stop_event.is_set() is False:
            try:
                await self.charger_topologie()
            except Exception:
                self.__logger.exception("entretien Erreur charger_topologie")
            try:
                await asyncio.wait_for(self.__stop_event.wait(), timeout=300)
            except asyncio.TimeoutError:
                pass  # OK

    async def entretien_store(self):
        pending = {asyncio.create_task(self.__stop_event.wait()), asyncio.create_task(self.__store_pret_event.wait())}

        # Attente configuration store
        done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        for d in done:
            if d.exception():
                raise d.exception()
        # Annuler tasks
        for p in pending:
            p.cancel()
            try:
                await p
            except asyncio.CancelledError:
                pass  # OK

        if self.__stop_event.is_set() is True:
            return  # Stopped

        if self.__etat_instance.est_primaire:
            self.__logger.info("Declencher sync initial (primaire)")
            self.__timestamp_dernier_sync = datetime.datetime.utcnow()
            await self.declencher_sync_primaire()
        else:
            self.__logger.info("Declencher sync initial (secondaire)")
            self.__timestamp_dernier_sync = datetime.datetime.utcnow()
            await self.declencher_sync_secondaire()

        # Boucle entretien
        while self.__stop_event.is_set() is False:
            try:
                await self.__store_consignation.run_entretien()
            except Exception:
                self.__logger.exception("entretien_store Erreur store_consignation.run()")

            try:
                await asyncio.wait_for(self.__stop_event.wait(), timeout=300)
            except asyncio.TimeoutError:
                pass  # OK

    async def visiter_fuuids(self, connection: SQLiteConnection):
        async with SQLiteDetachedVisiteAppend(connection) as detached_dao:
            await self.__store_consignation.visiter_fuuids(detached_dao)

    async def traiter_cedule(self, producer: MessageProducerFormatteur, message: MessageWrapper):
        self.__message_cedule = message
        self.__traiter_cedule_event.set()

    async def thread_traiter_cedule(self):
        self.__traiter_cedule_event = asyncio.Event()
        wait_coro = asyncio.create_task(self.__stop_event.wait())
        pending = {wait_coro}

        while self.__stop_event.is_set() is False:
            traiter_cedule_task = asyncio.create_task(self.__traiter_cedule_event.wait())
            pending.add(traiter_cedule_task)
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for d in done:
                if d.exception():
                    self.__logger.error("entretien_store Erreur tasK : %s" % d.exception())

            if self.__stop_event.is_set():
                break  # Termine

            self.__logger.debug("thread_traiter_cedule Debut")
            trigger = self.__message_cedule
            self.__message_cedule = None
            if self.__etat_instance.est_primaire:
                try:
                    await self.__traiter_cedule_primaire(trigger)
                except Exception:
                    self.__logger.exception("thread_traiter_cedule Erreur __traiter_cedule_primaire")
            try:
                await self.__traiter_cedule_local(trigger)
            except Exception:
                self.__logger.exception("thread_traiter_cedule Erreur __traiter_cedule_local")

            self.__logger.debug("thread_traiter_cedule Fin")
            self.__traiter_cedule_event.clear()

    async def __traiter_cedule_primaire(self, trigger: MessageWrapper):
        self.__logger.debug("__traiter_cedule_primaire Debut")

        now = datetime.datetime.utcnow()
        config_topologie = self.__etat_instance.topologie
        intervalle_sync_secs = config_topologie.get('sync_intervalle') or Constantes.CONST_DEFAUT_SYNC_INTERVALLE
        intervalle_sync = datetime.timedelta(seconds=intervalle_sync_secs)
        if self.__timestamp_dernier_sync is None or now - intervalle_sync > self.__timestamp_dernier_sync:
            self.__logger.info("__traiter_cedule_primaire Demarrer sync primaire")
            self.__timestamp_dernier_sync = datetime.datetime.utcnow()
            self.__sync_manager.demarrer_sync_primaire()

        self.__logger.debug("__traiter_cedule_primaire Fin")

    async def __traiter_cedule_local(self, trigger: MessageWrapper):
        self.__logger.debug("__traiter_cedule_local Debut")
        # now = datetime.datetime.utcnow()
        cedule_trigger = datetime.datetime.fromtimestamp(trigger.parsed['estampille'], tz=pytz.UTC)
        cedule_minute = cedule_trigger.minute

        try:
            await asyncio.wait_for(self.__store_pret_event.wait(), timeout=10)
        except asyncio.TimeoutError:
            self.__logger.info("__traiter_cedule_local Timeout attente store pret - SKIP")
            return

        if self.__sync_manager.sync_en_cours:
            self.__logger.debug("__traiter_cedule_local Sync en cours, skip reste du traitement")
            return

        # if self.__timestamp_verification is None or now - self.__intervalle_verification > self.__timestamp_verification:
        if cedule_minute % 20 == 1:  # Aux 20 minutes
            try:
                # Demarrer la job si le semaphore n'est pas deja bloque
                if self.__sync_manager.sync_en_cours is False:
                    self.__logger.info("__traiter_cedule_local Verifier fuuids")
                    self.__timestamp_verification = datetime.datetime.utcnow()
                    await self.__store_consignation.verifier_fuuids()
            except Exception:
                self.__logger.exception("__traiter_cedule_local Erreur verifier fuuids")

        # if self.__timestamp_orphelins is None or now - self.__intervalle_orphelins > self.__timestamp_orphelins:
        if cedule_minute == 28:  # Une fois par heure
            try:
                # Demarrer la job si le semaphore n'est pas deja bloque
                self.__logger.info("__traiter_cedule_local Supprimer orphelins")
                self.__timestamp_orphelins = datetime.datetime.utcnow()
                await self.__store_consignation.supprimer_orphelins()
            except Exception:
                self.__logger.exception("__traiter_cedule_local Erreur supprimer_orphelins")

        self.__logger.debug("__traiter_cedule_local Fin")

    async def thread_emettre_etat(self):

        await asyncio.wait_for(self.__etat_instance.producer_wait(), timeout=20)

        while self.__stop_event.is_set() is False:
            try:
                await self.emettre_etat()
            except sqlite3.OperationalError:
                self.__logger.debug("thread_emettre_etat DB Locked, etat n'est pas emis")
            except Exception:
                self.__logger.exception("Erreur emettre_etat")
            try:
                await asyncio.wait_for(self.__stop_event.wait(), timeout=90)
            except asyncio.TimeoutError:
                pass  # OK

    async def ouvrir_sessions(self):
        if self.__session_http_download is None or self.__session_http_download.closed:
            timeout = aiohttp.ClientTimeout(connect=5, total=300)
            self.__session_http_download = aiohttp.ClientSession(timeout=timeout)

        if self.__session_http_requests is None or self.__session_http_requests.closed:
            timeout_requests = aiohttp.ClientTimeout(connect=5, total=15)
            self.__session_http_requests = aiohttp.ClientSession(timeout=timeout_requests)

    async def modifier_topologie(self, configuration_topologie: dict):
        await self.__etat_instance.maj_topologie(configuration_topologie)

        producer = self.__etat_instance.producer
        await asyncio.wait_for(producer.producer_pret().wait(), 3)
        try:
            if self.__etat_instance.topologie.get('primaire') is True:
                # Faire un lien direct entre primaire et topologie (meme reference)
                self.__etat_instance.primaire = self.__etat_instance.topologie
            else:
                # Charger l'information sur la consignation primaire
                requete = {'primaire': True}
                reponse = await producer.executer_requete(
                    requete, 'CoreTopologie', 'getConsignationFichiers', exchange="2.prive")
                info_primaire = reponse.parsed
                if info_primaire['ok'] is True:
                    self.__etat_instance.primaire = info_primaire

        except Exception as e:
            self.__logger.exception("Erreur chargement consignation primaire")

        est_primaire = self.__etat_instance.est_primaire
        if est_primaire:
            # Activer le consumer sur Q fichiers/primaire
            await self.rabbitmq_dao.mq_thread.start_consumer('fichiers/primaire')
        else:
            # Desactiver le consumer sur Q fichiers/primaire
            await self.rabbitmq_dao.mq_thread.stop_consumer('fichiers/primaire')
            # Mettre a jour la consignation primaire
            await self.__etat_instance.charger_consignation_primaire()

        type_store = self.__etat_instance.topologie['type_store']
        class_type = map_type(type_store)

        if self.__store_consignation is None or self.__store_consignation.__class__ != class_type:
            # Arreter store courant si present
            self.__logger.info("modifier_topologie Changer store consignation pour type %s" % type_store)
            if self.__store_consignation is not None:
                self.__logger.info("modifier_topologie Arret store consignation courant (stop)")
                await self.__store_consignation.stop()
            instance_store = class_type(self.__etat_instance)
            self.__store_consignation = instance_store
            self.__store_consignation.initialiser()

        # Appliquer le changement de configuration de backup au besoin
        await self.__store_backup.changement_topologie()

        # La configuration du store est prete
        self.__store_pret_event.set()

    async def charger_topologie(self):
        # Indiquer qu'on rafrachi la topologie
        self.__store_pret_event.clear()

        """ Charge la configuration a partir de CoreTopologie """
        producer = self.__etat_instance.producer
        if producer is None:
            await asyncio.sleep(5)  # Attendre connexion MQ
            producer = self.__etat_instance.producer
            if producer is None:
                raise Exception('producer pas pret')

        await asyncio.wait_for(producer.producer_pret().wait(), 30)

        instance_id = self.__etat_instance.clecertificat.enveloppe.subject_common_name
        requete = {'instance_id': instance_id}
        reponse = await producer.executer_requete(
            requete, 'CoreTopologie', 'getConsignationFichiers', exchange="2.prive")

        if reponse.parsed['ok'] is True:
            # Conserver configuration topologie - dechiffrer partie chiffree
            configuration_topologie = reponse.parsed
            await self.modifier_topologie(configuration_topologie)
        elif self.etat_instance.topologie is not None:
            # Conserver la configuration courante
            self.__logger.warning("charger_topologie Erreur chargement configuration de consignation: %s" % reponse.parsed)
        else:
            # Aucune configuration connue pour l'instance
            # Mettre configuration par defaut et sauvegarder aupres de CoreTopologie
            await self.initialiser_nouvelle_consignation()

    async def emettre_etat(self):
        producer = self.__etat_instance.producer
        if producer is None or self.__etat_instance.topologie is None:
            await asyncio.sleep(5)  # Attendre connexion MQ, chargement de la configuration
            producer = self.__etat_instance.producer
            if producer is None:
                raise Exception('producer pas pret')
        await asyncio.wait_for(producer.producer_pret().wait(), 15)

        etat = {}
        configuration_topologie = self.__etat_instance.topologie
        for champ in Constantes.CONST_CHAMPS_CONFIG:
            try:
                val = configuration_topologie[champ]
                etat[champ] = val
            except (TypeError, KeyError):
                pass

        # Charger etat des fichiers (taille totale par type)
        if self.__store_consignation is None:
            try:
                await asyncio.wait_for(self.__store_pret_event.wait(), 3)
            except asyncio.TimeoutError:
                pass

        if self.__store_consignation is not None:
            stats = await self.__store_consignation.get_stats()
            etat.update(stats)

        # Charger etat downloads et uploads si secondaire

        await self.__etat_instance.producer.emettre_evenement(
            etat, Constantes.DOMAINE_FICHIERS, Constantes.EVENEMENT_PRESENCE,
            exchanges=ConstantesMillegrilles.SECURITE_PRIVE)

    async def initialiser_nouvelle_consignation(self):
        configuration_topologie_defaut = {
            'type_store': 'millegrille',
            # 'consignation_url': 'https://fichiers:1443',
        }
        await self.modifier_topologie(configuration_topologie_defaut)

        await self.emettre_etat()

    async def consigner(self, path_source: pathlib.Path, fuuid: str):
        if self.__store_consignation is None:
            raise StoreNonInitialise("Store non initialise")
        await self.__store_consignation.consigner(path_source, fuuid)

    async def emettre_evenement_consigne(self, fuuid: str):
        """
        Emet l'evenement consigne (visite).
        :param fuuid:
        :return:
        """
        await self.__store_consignation.emettre_evenement_consigne(fuuid)

    async def get_info_fichier(self, fuuid: str):
        if self.__store_consignation is None:
            raise Exception("Store non initialise")
        return await self.__store_consignation.get_info_fichier(fuuid)

    async def get_info_fichier_backup(self, uuid_backup: str, domaine: str, nom_fichier: str):
        return await self.__store_consignation.get_info_fichier_backup(uuid_backup, domaine, nom_fichier)

    async def stream_fuuid(self, fuuid: str, response: web.StreamResponse, start: Optional[int] = None, end: Optional[int] = None):
        if self.__store_consignation is None:
            raise Exception("Store non initialise")
        return await self.__store_consignation.stream_response_fuuid(fuuid, response, start, end)

    async def conserver_backup(self, fichier_temp: tempfile.TemporaryFile, uuid_backup: str, domaine: str, nom_fichier: str):
        await self.__store_consignation.conserver_backup(fichier_temp, uuid_backup, domaine, nom_fichier)

    async def stream_backup(self, response: web.StreamResponse, uuid_backup: str, domaine: str, fichier_nom: str):
        if self.__store_consignation is None:
            raise Exception("Store non initialise")
        return await self.__store_consignation.stream_backup(response, uuid_backup, domaine, fichier_nom)

    async def rotation_backup(self, commande: dict):
        raise NotImplementedError('obsolete')
        # self.__logger.debug("Rotation backup %s" % commande)
        #
        # # Verifier que la commande vient d'un module de backup
        # enveloppe = await self.__etat_instance.validateur_message.verifier(commande['__original'])
        # if ConstantesMillegrilles.ROLE_BACKUP not in enveloppe.get_roles:
        #     return {'ok': False, 'err': 'rotation_backup acces refuse (role invalide)'}
        #
        # # Supprimer les backups qui ne sont pas dans la liste
        # uuid_backups = commande['uuid_backups']
        # if self.__store_consignation is not None:
        #     await self.__store_consignation.rotation_backups(uuid_backups)
        #     return {'ok': True}
        # else:
        #     return {'ok': False, 'err': 'Consignation non prete'}

    async def declencher_sync_primaire(self, commande: Optional[dict] = None):
        if self.__store_consignation is None:
            return {'ok': False, 'err': 'Message sync - store consignation n\'est pas pret'}

        if self.__etat_instance.est_primaire is True:
            self.__sync_manager.demarrer_sync_primaire()
            return {'ok': True}

        return {'ok': False, 'err': 'Pas primaire'}

    async def declencher_sync_secondaire(self, commande: Optional[dict] = None):
        if self.__store_consignation is None:
            return {'ok': False, 'err': 'Message sync - store consignation n\'est pas pret'}

        if self.__etat_instance.est_primaire is False:
            self.__sync_manager.demarrer_sync_secondaire()
            return {'ok': True}
        else:
            return {'ok': False, 'err': 'Pas secondaire'}

    async def conserver_activite_fuuids(self, commande: dict):
        await self.__sync_manager.conserver_activite_fuuids(commande)

    async def reactiver_fuuids(self, commande: dict):
        await self.__store_pret_event.wait()
        return await self.__store_consignation.reactiver_fuuids(commande)

    async def upload_backups_primaire(self, connection_transfert: SQLiteConnection, session: aiohttp.ClientSession):
        await self.__store_pret_event.wait()
        # await self.__store_consignation.upload_backups_primaire(connection_transfert, session)
        await self.__store_consignation.upload_backups_v2_primaire(connection_transfert, session)

    async def reset_transferts_secondaires(self, commande: dict):
        await self.__sync_manager.run_entretien_transferts(reset=True)
        return {'ok': True}

    @asynccontextmanager
    async def get_fp_fuuid(self, fuuid: str, start: Optional[int] = None):
        fichier = None
        try:
            fichier = await self.__store_consignation.get_fp_fuuid(fuuid, start)
            yield fichier
        finally:
            if fichier:
                fichier.close()

    @asynccontextmanager
    async def get_fp_backup(self, uuid_backup: str, domaine: str, fichier_nom: str, start: Optional[int] = None):
        fichier = None
        try:
            fichier = await self.__store_consignation.get_fp_backup(uuid_backup, domaine, fichier_nom, start)
            yield fichier
        finally:
            if fichier:
                fichier.close()

    async def get_domaines_backups(self):
        async for backup in self.__store_consignation.get_domaines_backups():
            yield backup

    @property
    def stop_event(self) -> asyncio.Event:
        return self.__stop_event

    @property
    def etat_instance(self):
        return self.__etat_instance

    @property
    def rabbitmq_dao(self) -> Optional[MilleGrillesConnecteur]:
        return self.__rabbitmq_dao

    @rabbitmq_dao.setter
    def rabbitmq_dao(self, rabbitmq_dao: MilleGrillesConnecteur):
        self.__rabbitmq_dao = rabbitmq_dao

    @property
    def timestamp_visite(self):
        return self.__timestamp_visite

    @timestamp_visite.setter
    def timestamp_visite(self, ts):
        self.__timestamp_visite = ts

    async def reclamer_fuuids_database(self, fuuids: list, bucket: str):
        if self.__store_consignation is not None:
            await self.__store_consignation.reclamer_fuuids_database(fuuids, bucket)
        else:
            self.__logger.warning("reclamer_fuuids_database Reception message avant initialisation store")

    async def generer_reclamations_sync(self, connection: SQLiteConnection):
        if self.__store_consignation is not None:
            await self.__store_consignation.generer_reclamations_sync(connection)
        else:
            self.__logger.warning("generer_reclamations_sync Reception message avant initialisation store")

    async def generer_backup_sync(self):
        if self.__store_consignation is not None:
            await self.__store_consignation.generer_backup_sync()
        else:
            self.__logger.warning("generer_backup_sync Reception message avant initialisation store")

    async def ajouter_fuuid_primaire(self, commande: dict):
        """ Ajoute un fichier qui a ete consigne par le primaire """
        if self.__etat_instance.est_primaire:
            return  # Rien a faire, on est primaire

        await self.__sync_manager.ajouter_fichier_primaire(commande)

    async def ajouter_upload_secondaire(self, fuuid: str):
        """ Ajoute conditionnelement un fuuid a uploader vers le primaire """
        await self.__sync_manager.ajouter_upload_secondaire(fuuid)

    @property
    def sync_en_cours(self):
        return self.__sync_manager.sync_en_cours

    async def put_backup_v2_fichier(self, fichier_temp: tempfile.TemporaryFile,
                                    domaine: str, nom_fichier: str, type_fichier: str, version: Optional[str] = None):
        if self.__store_consignation is not None:
            return await self.__store_consignation.put_backup_v2_fichier(fichier_temp, domaine, nom_fichier, type_fichier, version)
        else:
            raise StoreNonInitialise("Store non initialise")

    async def get_backup_v2_fichier_stream(self, domaine: str, nom_fichier: str, version: Optional[str] = None):
        if self.__store_consignation is not None:
            return await self.__store_consignation.get_backup_v2_fichier_stream(domaine, nom_fichier, version)
        else:
            raise StoreNonInitialise("Store non initialise")

    async def get_backup_v2_versions(self, domaine: str) -> dict:
        if self.__store_consignation is not None:
            return await self.__store_consignation.get_backup_v2_versions(domaine)
        else:
            raise StoreNonInitialise("Store non initialise")

    async def get_backup_v2_list(self, domaine, version: Optional[str] = None) -> list[str]:
        if self.__store_consignation is not None:
            return await self.__store_consignation.get_backup_v2_list(domaine, version)
        else:
            raise StoreNonInitialise("Store non initialise")

    async def get_backup_v2_headers(self, domaine, version: Optional[str] = None):
        if self.__store_consignation is not None:
            return await self.__store_consignation.get_backup_v2_headers(domaine, version)
        else:
            raise StoreNonInitialise("Store non initialise")

    async def get_backup_v2_domaines(self, domaines: Optional[list[str]] = None, courant=True, stats=False,
                                     cles=False):
        if self.__store_consignation is not None:
            return await self.__store_consignation.get_backup_v2_domaines(domaines, courant, stats, cles)
        else:
            raise StoreNonInitialise("Store non initialise")

    async def rotation_backups_v2(self):
        if self.__store_consignation is not None:
            return await self.__store_consignation.rotation_backups_v2()
        else:
            raise StoreNonInitialise("Store non initialise")

    async def thread_sync_backups_v2_secondaire(self):
        while self.__stop_event.is_set() is False:
            try:
                await asyncio.wait_for(self.etat_instance.backup_event.wait(), 5)

                url_consignation_primaire = await self.__etat_instance.charger_consignation_primaire()
                url_backup = '%s/fichiers_transfert/backup_v2' % url_consignation_primaire

                self.etat_instance.backup_event.clear()  # Clear backup flag
                timeout = aiohttp.ClientTimeout(connect=5, total=600)
                ssl_context = self.__etat_instance.ssl_context
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    await self.__store_consignation.sync_backups_v2_primaire(session, url_backup, ssl_context)
            except TimeoutError:
                pass

            if self.__stop_event.is_set():
                return  # Done

    # async def download_fichier(self, fuuid, cle_chiffree, params_dechiffrage, path_destination):
    #     await self.ouvrir_sessions()  # S'assurer d'avoir une session ouverte
    #     url_fuuid = self.get_url_fuuid(fuuid)
    #
    #     clecert = self.__etat_instance.clecertificat
    #     decipher = get_decipher(clecert, cle_chiffree, params_dechiffrage)
    #
    #     timeout = aiohttp.ClientTimeout(connect=5, total=600)
    #     with path_destination.open(mode='wb') as output_file:
    #         async with aiohttp.ClientSession(timeout=timeout) as session:
    #             async with session.get(url_fuuid, ssl=self.__etat_instance.ssl_context) as resp:
    #                 resp.raise_for_status()
    #
    #                 async for chunk in resp.content.iter_chunked(64*1024):
    #                     output_file.write(decipher.update(chunk))
    #
    #         output_file.write(decipher.finalize())
    #
    # async def verifier_existance(self, fuuid: str) -> dict:
    #     """
    #     Requete HEAD pour verifier que le fichier existe sur la consignation locale.
    #     :param fuuid:
    #     :return:
    #     """
    #     await self.ouvrir_sessions()  # S'assurer d'avoir une session ouverte
    #     url_fuuid = self.get_url_fuuid(fuuid)
    #     reponse = await self.__session_http_requests.head(url_fuuid, ssl=self.__etat_instance.ssl_context)
    #     return {'taille': reponse.headers.get('Content-Length'), 'status': reponse.status}
    #
    # def get_url_fuuid(self, fuuid):
    #     return f"{self.__url_consignation}/fichiers_transfert/{fuuid}"
