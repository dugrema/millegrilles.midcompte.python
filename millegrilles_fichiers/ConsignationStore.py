import asyncio
import datetime
import errno
import gzip
import json
import logging
import pathlib
import math
import shutil
import tempfile

from io import BufferedReader, BufferedIOBase
from os import makedirs
from math import floor
from ssl import SSLContext

from aiohttp import web, ClientSession
from typing import Optional, Type, Union, BinaryIO

import pytz

from millegrilles_fichiers.BackupV2 import lire_header_archive_backup, sync_backups_v2_primaire, rotation_backups_v2
from millegrilles_fichiers.SQLiteDao import SQLiteConnection
from millegrilles_messages.messages import Constantes as ConstantesMillegrilles
from millegrilles_messages.messages.Hachage import VerificateurHachage, ErreurHachage

from millegrilles_fichiers import Constantes
from millegrilles_fichiers.EtatFichiers import EtatFichiers
from millegrilles_fichiers.UploadFichiersPrimaire import EtatUpload, feed_filepart2
from millegrilles_fichiers.SQLiteDao import (SQLiteReadOperations, SQLiteWriteOperations,
                                             SQLiteDetachedReclamationAppend, SQLiteDetachedVisiteAppend)


CHUNK_SIZE = 1024 * 64


class ConsignationStore:

    def __init__(self, etat: EtatFichiers):
        self._etat = etat
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

        # Creer repertoire data, path database
        path_database = pathlib.Path(
            self._etat.configuration.dir_data, Constantes.FICHIER_DATABASE_FICHIERS)
        self.__path_database = path_database
        self._path_backup_v2 = path_backup_v2 = pathlib.Path(
            self._etat.configuration.dir_consignation, Constantes.DIR_BACKUP_V2)

        self._stop_store: Optional[asyncio.Event] = None

    def initialiser(self):
        self._stop_store = asyncio.Event()

        path_backup = pathlib.Path(
            self._etat.configuration.dir_consignation, Constantes.DIR_BACKUP)
        path_backup.mkdir(parents=True, exist_ok=True)

        self._path_backup_v2.mkdir(parents=True, exist_ok=True)

        self.__path_database.parent.mkdir(parents=True, exist_ok=True)
        with self._etat.sqlite_connection() as connection:
            connection.init_database()

    async def run_entretien(self):
        raise NotImplementedError('must override')

    async def stop(self):
        self._stop_store.set()
        self._etat.backup_event.set()

    def get_path_fuuid(self, bucket: str, fuuid: str) -> pathlib.Path:
        raise NotImplementedError('must override')

    async def __consigner_db(self, path_src: pathlib.Path, fuuid: str):
        stat = path_src.stat()
        with self._etat.sqlite_connection() as connection:
            async with SQLiteWriteOperations(connection) as dao:
                await asyncio.to_thread(dao.consigner, fuuid, stat.st_size, Constantes.BUCKET_PRINCIPAL)

    async def consigner(self, path_src: pathlib.Path, fuuid: str):
        # Tenter d'inserer le fichier comme nouveau actif dans la base de donnees
        await self.__consigner_db(path_src, fuuid)

        # await self.emettre_batch_visites([fuuid], verification=True)
        await self.emettre_evenement_consigne(fuuid)

    async def changer_bucket(self, fuuid: str, bucket: str):
        """ Deplace le fichier vers un bucket """
        raise NotImplementedError('must override')

    async def supprimer(self, bucket: str, fuuid: str):
        """ Marquer orphelin si fichier est actif """
        raise NotImplementedError('must override')

    async def recuperer(self, fuuid: str):
        """ Marquer le fichier orphelin comme actif """
        raise NotImplementedError('must override')

    async def purger(self, fuuid: str):
        """ Retirer le fichier du bucket """
        raise NotImplementedError('must override')

    async def stream_response_fuuid(self, fuuid: str, response: web.StreamResponse, start: Optional[int] = None, end: Optional[int] = None):
        """ Stream les bytes du fichier en utilisant la response """
        raise NotImplementedError('must override')

    async def stream_backup(self, response: web.StreamResponse, uuid_backup: str, domaine: str, fichier_nom: str):
        """ Stream les bytes du fichier en utilisant la response """
        raise NotImplementedError('must override')

    async def get_fp_fuuid(self, fuuid, start: Optional[int] = None):
        """ Retourne un file pointer ou equivalent pour acceder a un fuuid. """
        raise NotImplementedError('must override')

    async def get_fp_backup(self, uuid_backup: str, domaine: str, fichier_nom: str, start: Optional[int] = None):
        """ Retourne un file pointer ou equivalent pour acceder a fichier de backup. """
        raise NotImplementedError('must override')

    async def entretien(self):
        """ Declenche un entretien (visite, verification, purge, etc.) """
        raise NotImplementedError('must override')

    async def visiter_fuuids(self, dao: SQLiteDetachedVisiteAppend):
        dir_buckets = pathlib.Path(self._etat.configuration.dir_consignation, Constantes.DIR_BUCKETS)
        self.__logger.info("visiter_fuuids Debut avec path buckets %s" % dir_buckets)

        # Parcourir tous les buckets recursivement (depth-first)
        for bucket in dir_buckets.iterdir():
            self.__logger.debug("Visiter bucket %s" % bucket.name)
            await self.visiter_bucket(bucket.name, bucket, dao)

        # Commit de la derniere batch
        batch, resultat = await dao.commit_batch()
        if batch:
            # Emettre dernier message de visite
            liste_fuuids = [f['fuuid'] for f in batch]
            await self.emettre_batch_visites(liste_fuuids, False)

        self.__logger.info("visiter_fuuids Fin")

    async def visiter_bucket(self, bucket: str, path_repertoire: pathlib.Path, dao: SQLiteDetachedVisiteAppend):
        """ Visiter tous les fichiers presents, s'assurer qu'ils sont dans la base de donnees. """
        raise NotImplementedError('must override')

    async def get_info_fichier_backup(self, uuid_backup: str, domaine: str, nom_fichier: str) -> dict:
        """ Retourne l'information d'un fichier de backup ou FileNotFoundError si non trouve. """
        raise NotImplementedError('obsolete')

    async def verifier_fuuids(self, limite=Constantes.CONST_LIMITE_TAILLE_VERIFICATION):
        await self.__verifier_fuuids(limite)

    async def __verifier_fuuids(self, limite=Constantes.CONST_LIMITE_TAILLE_VERIFICATION):
        """ Visiter tous les fichiers presents, s'assurer qu'ils sont dans la base de donnees. """
        with self._etat.sqlite_connection(readonly=True) as connection:
            async with SQLiteReadOperations(connection) as dao_read:
                liste_fichiers = await asyncio.to_thread(dao_read.charger_verifier_fuuids, limite)

        # Verifier chaque fichier individuellement
        # On conserve le lock sur operations de batch pour la duree du traitement
        for fichier in liste_fichiers:
            await self.verifier_fichier(**fichier)

    async def supprimer_orphelins(self):
        producer = self._etat.producer
        await asyncio.wait_for(producer.producer_pret().wait(), timeout=10)

        est_primaire = self._etat.est_primaire
        if est_primaire:
            expiration_secs = Constantes.CONST_EXPIRATION_ORPHELIN_PRIMAIRE
        else:
            expiration_secs = Constantes.CONST_EXPIRATION_ORPHELIN_SECONDAIRE
        expiration = datetime.datetime.now(tz=pytz.UTC) - datetime.timedelta(seconds=expiration_secs)
        self.__logger.info("supprimer_orphelins Expires depuis %s" % expiration)

        # Operations de lectures
        with self._etat.sqlite_connection(readonly=True) as connection:
            async with SQLiteReadOperations(connection) as dao_read:
                batch_orphelins = await asyncio.to_thread(dao_read.identifier_orphelins, expiration)

        fuuids = [f['fuuid'] for f in batch_orphelins]
        fuuids_a_supprimer = set(fuuids)

        if len(fuuids_a_supprimer) > 0:
            self.__logger.info("Supprimer fuuids %s" % fuuids_a_supprimer)

            if est_primaire:
                # Transmettre commande pour confirmer que la suppression finale peut avoir lieu
                commande = {'fuuids': fuuids}
                reponse = await producer.executer_commande(
                    commande,
                    domaine=Constantes.DOMAINE_GROSFICHIERS, action=Constantes.COMMANDE_SUPPRIMER_ORPHELINS,
                    exchange=ConstantesMillegrilles.SECURITE_PRIVE, timeout=90
                )
                if reponse.parsed['ok'] is True:
                    # Retirer tous les fuuids a conserver de la liste a supprimer
                    fuuids_conserver = reponse.parsed.get('fuuids_a_conserver') or list()
                    fuuids_a_supprimer = fuuids_a_supprimer.difference(fuuids_conserver)

            for fichier in batch_orphelins:
                fuuid = fichier['fuuid']
                bucket = fichier['bucket']
                if fuuid in fuuids_a_supprimer:
                    if bucket is not None:
                        self.__logger.info("supprimer_orphelins Supprimer fichier orphelin expire %s" % fichier)
                        try:
                            await self.supprimer(bucket, fuuid)
                        except OSError as e:
                            if e.errno == errno.ENOENT:
                                self.__logger.warning("Erreur suppression fichier %s/%s - non trouve" % (bucket, fuuid))
                            else:
                                raise e
                    else:
                        self.__logger.info("supprimer_orphelins Nettoyer DB pour fichier orphelin deja supprime %s" % fichier)

            # Supprimer de la base de donnes locale
            with self._etat.sqlite_connection() as connection:
                # Ouvrir l'operation de batch (lock)
                async with SQLiteWriteOperations(connection) as dao:
                    await dao.supprimer(fuuids_a_supprimer)

        pass

    async def verifier_fichier(self, fuuid: str, taille: int, bucket: str):
        """ Verifie un fichier individuellement. Marque resultat dans la base de donnees. """
        raise NotImplementedError('must override')

    async def conserver_backup(self, fichier_temp: tempfile.TemporaryFile, uuid_backup: str, domaine: str,
                               nom_fichier: str):
        raise NotImplementedError('obsolete')

    async def rotation_backups(self, uuid_backups_conserver: list[str]):
        """ Supprime tous les backups qui ne sont pas dans la liste """
        raise NotImplementedError('obsolete')

    async def get_stat_fichier(self, fuuid: str, bucket: Optional[str] = None) -> Optional[dict]:
        """
        Retourne de l'information de base d'un fichier
        :param fuuid:
        :param bucket:
        :return: dict ou None
        """
        raise NotImplementedError('must override')

    async def reactiver_fuuids(self, commande: dict):
        """ Reactiver un fuuid (orphelin -> actif) si applicable. Retourne """
        fuuids = commande['fuuids']
        sur_echec = commande.get('sur_echec') or False

        fuuids_actifs = await self.__reactiver_fuuids(fuuids)

        if len(fuuids_actifs) == 0 and sur_echec is False:
            return  # Aucun message de retour

        # Verifier l'existence des fichiers
        fuuids_inconnus = set(fuuids)
        fuuids_trouves = list()
        erreurs = list()
        for fuuid in fuuids_actifs:
            fuuids_inconnus.remove(fuuid)  # Retirer de la liste inconnu - va etre actif ou erreur
            # Acceder au fichier sur disque
            info = await self.get_stat_fichier(fuuid)
            if info is not None:
                fuuids_trouves.append(fuuid)
            else:
                erreurs.append({'fuuid': fuuid, 'code': 1, 'err': 'FileNotFound'})

        reponse = {
            'recuperes': fuuids_actifs,
            'inconnus': list(fuuids_inconnus),
            'errors': erreurs,
        }

        return reponse

    async def __reactiver_fuuids(self, fuuids: list) -> list[str]:
        date_reclamation = datetime.datetime.now(tz=pytz.UTC)

        with self._etat.sqlite_connection() as connection:
            async with SQLiteWriteOperations(connection) as dao_write:
                fuuid_keys = await asyncio.to_thread(dao_write.activer_si_orphelin, fuuids, date_reclamation)

            async with SQLiteReadOperations(connection) as dao_read:
                liste_fichiers_actifs = await asyncio.to_thread(dao_read.get_info_fichiers_actif, fuuid_keys)

        return liste_fichiers_actifs

    async def get_stats(self):
        with self._etat.sqlite_connection(readonly=True) as connection:
            async with SQLiteReadOperations(connection) as dao_read:
                return await asyncio.to_thread(dao_read.get_stats_fichiers)

    async def get_info_fichier(self, fuuid: str) -> Optional[dict]:
        with self._etat.sqlite_connection(readonly=True) as connection:
            async with SQLiteReadOperations(connection) as dao_read:
                return await asyncio.to_thread(dao_read.get_info_fichier, fuuid)

    async def emettre_batch_visites(self, fuuids: list[Union[str, dict]], verification=False):
        fuuids_parsed = list()
        for r in fuuids:
            if isinstance(r, dict):
                fuuids_parsed.append(r['fuuid'])
            elif isinstance(r, str):
                fuuids_parsed.append(r)
            else:
                raise TypeError('doit etre str ou dict["fuuid"]')

        producer = self._etat.producer
        await asyncio.wait_for(producer.producer_pret().wait(), timeout=10)

        message = {
            'fuuids': fuuids_parsed,
            'verification': verification,
        }
        await producer.emettre_evenement(
            message,
            domaine=Constantes.DOMAINE_FICHIERS, action=Constantes.EVENEMENT_VISITER_FUUIDS,
            exchanges=ConstantesMillegrilles.SECURITE_PRIVE
        )

    async def emettre_evenement_consigne(self, fuuid: str):
        producer = self._etat.producer
        await asyncio.wait_for(producer.producer_pret().wait(), timeout=10)

        message = {'hachage_bytes': fuuid}
        await producer.emettre_evenement(
            message,
            domaine=Constantes.DOMAINE_FICHIERS, action=Constantes.EVENEMENT_FICHIER_CONSIGNE,
            exchanges=ConstantesMillegrilles.SECURITE_PRIVE
        )

        if self._etat.est_primaire:
            message = {'fuuid': fuuid}
            await producer.emettre_evenement(
                message,
                domaine=Constantes.DOMAINE_FICHIERS, action=Constantes.EVENEMENT_CONSIGNATION_PRIMAIRE,
                exchanges=ConstantesMillegrilles.SECURITE_PRIVE
            )

    async def reclamer_fuuids_database(self, fuuids: list, bucket: str):
        path_db_sync = pathlib.Path(self._etat.get_path_data(), Constantes.FICHIER_DATABASE_SYNC)
        locks = self._etat.sqlite_locks
        with SQLiteConnection(path_db_sync, locks=locks, check_same_thread=False, timeout=60.0) as connection:
            async with SQLiteDetachedReclamationAppend(connection) as detached_dao:
                await detached_dao.reclamer_fuuids(fuuids, bucket)

    async def generer_reclamations_sync(self, connection: SQLiteConnection):
        dir_data = pathlib.Path(self._etat.configuration.dir_data)
        fichier_reclamations = pathlib.Path(dir_data, Constantes.FICHIER_RECLAMATIONS_PRIMAIRES)

        # Preparer le fichier work
        fichier_reclamations_work = pathlib.Path('%s.work' % fichier_reclamations)
        fichier_reclamations_work.unlink(missing_ok=True)

        async with SQLiteReadOperations(connection) as dao:
            try:
                with gzip.open(fichier_reclamations_work, 'wt') as fichier:
                    await asyncio.to_thread(dao.generer_relamations_primaires, fichier)

                # Renommer fichier .work pour remplacer le fichier de reclamations precedent
                fichier_reclamations.unlink(missing_ok=True)
                fichier_reclamations_work.rename(fichier_reclamations)
            except:
                self.__logger.exception('Erreur generation fichier reclamations')
                fichier_reclamations_work.unlink(missing_ok=True)

    async def generer_backup_sync(self):
        """ Genere le fichier backup.jsonl.gz """
        raise NotImplementedError('must implement')

    async def upload_backups_primaire(self, connection_transfert: SQLiteConnection, session: ClientSession):
        raise NotImplementedError('obsolete')

    async def sync_backups_v2_primaire(self, session: ClientSession, url_backup: str, ssl_context: SSLContext):
        raise NotImplementedError('must implement')

    # async def upload_backup_primaire(self, session: ClientSession, uuid_backup: str, domaine: str, nom_fichier: str, fichier):
    #     url_consignation_primaire = await self._etat.charger_consignation_primaire()
    #     url_backup = '%s/fichiers_transfert/backup/upload' % url_consignation_primaire
    #     url_fichier = f"{url_backup}/{uuid_backup}/{domaine}/{nom_fichier}"
    #
    #     etat_upload = EtatUpload('', fichier, self._stop_store, 0)
    #
    #     feeder = feed_filepart2(etat_upload, limit=10_000_000)
    #     session_coro = session.put(url_fichier, ssl=self._etat.ssl_context, data=feeder)
    #
    #     # Uploader chunk
    #     session_response = None
    #     try:
    #         session_response = await session_coro
    #         if etat_upload.done is False:
    #             raise Exception('Erreur upload fichier backup - feeder not done')
    #     finally:
    #         if session_response is not None:
    #             session_response.release()
    #             session_response.raise_for_status()

    async def get_domaines_backups(self):
        """ @:return Generateur par domaine des uuid de backups """
        raise NotImplementedError('must implement')

    # *********
    # Backup V2
    # *********

    async def put_backup_v2_fichier(self, fichier_temp: tempfile.TemporaryFile,
                                    domaine: str, nom_fichier: str, type_fichier: str, version: Optional[str] = None):
        """
        :param fichier_temp:
        :param domaine:
        :param nom_fichier:
        :param type_fichier: F pour final, C pour concatene, I pour incremental
        :param version: Si absent, doit etre un fichier final. Pour C et I doit etre present.
        :return:
        """
        raise NotImplementedError('must override')

    async def get_backup_v2_fichier_stream(self, domaine: str, nom_fichier: str, version: Optional[str] = None) -> BufferedReader:
        """
        :param domaine:
        :param nom_fichier: Fichier .mgbak
        :param version: Si absent, retourne un fichier final. Sinon utilise la version d'archive specifiee.
        :return:
        """
        raise NotImplementedError('must override')

    async def get_backup_v2_versions(self, domaine: str) -> dict:
        """
        :param domaine:
        :return: Liste de version disponibles pour ce domaine en format JSON. Identifie la versions courante.
        """
        raise NotImplementedError('must override')

    async def get_backup_v2_list(self, domaine, version: Optional[str] = None) -> list[str]:
        """
        Retourne une liste de fichiers du domaine specifie. Seul le nom des fichiers et fourni (pas de repertoire).
        :param domaine:
        :param version: Si aucune version n'est fournie, retourne la liste des fichiers finaux du domaine.
        :return: Liste texte utf-8 avec un fichier par ligne.
        """
        raise NotImplementedError('must override')

    async def get_backup_v2_headers(self, domaine, version: Optional[str] = None):
        """
        Retourne les headers presents dans le backup sous forme de 1 header JSON par ligne.
        :param domaine:
        :param version: Si absent, utilise les fichiers finaux. Sinon charge toutes les archives de la version specifiee.
        :return: Liste de headers json, 1 par ligne.
        """
        raise NotImplementedError('must override')

    async def get_backup_v2_domaines(self, domaines: Optional[list[str]] = None, courant=True, stats=False, cles=False):
        """
        Retourne une liste des domaines avec information
        :param domaines: Liste optionnelle de domaines a charger. Si None, retourne tous les domaines.
        :param courant: Si True, retourne uniquement les backup courants.
        :param comptes: Si True, inclue le nombre de transactions total par backup
        :param cles: Si True, inclue un dict de cles sous format: {cle_id: DomaineSignature}
        :return: Liste de domaines format: {domaine, date, version, nombre_transactions}
        """
        raise NotImplementedError('must override')

    async def rotation_backups_v2(self, nombre_archives=3):
        raise NotImplementedError('must override')

    # Backup V2 sync consignation
    # async def get_domaines_backups_v2(self):
    #     raise NotImplementedError('must implement')
    #
    # async def rotation_backups_v2(self, uuid_backups_conserver: list[str]):
    #     """ Supprime tous les backups qui ne sont pas dans la liste """
    #     raise NotImplementedError('must override')
    #
    # async def generer_backup_sync_v2(self):
    #     """ Genere le fichier backup.jsonl.gz """
    #     raise NotImplementedError('must implement')
    #
    # async def upload_backups_primaire_v2(self, connection_transfert: SQLiteConnection, session: ClientSession):
    #     raise NotImplementedError('must implement')
    #
    # async def upload_backup_primaire_v2(self, session: ClientSession, uuid_backup: str, domaine: str, nom_fichier: str, fichier):
    #     raise NotImplementedError('must implement')


class ConsignationStoreMillegrille(ConsignationStore):

    def __init__(self, etat: EtatFichiers):
        super().__init__(etat)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

    def get_path_fuuid(self, bucket: str, fuuid: str) -> pathlib.Path:
        dir_consignation = pathlib.Path(self._etat.configuration.dir_consignation)
        sub_folder = fuuid[-2:]
        path_fuuid = pathlib.Path(dir_consignation, Constantes.DIR_BUCKETS, bucket, sub_folder, fuuid)
        return path_fuuid

    def get_path_backups(self):
        return pathlib.Path(self._etat.configuration.dir_consignation, Constantes.DIR_BACKUP)

    async def consigner(self, path_src: pathlib.Path, fuuid: str):
        path_dest = self.get_path_fuuid(Constantes.BUCKET_PRINCIPAL, fuuid)
        if path_dest.exists() is False:
            # Tenter de deplacer avec rename
            try:
                path_dest.parent.mkdir(parents=True, exist_ok=True)
                path_src.rename(path_dest)
            except OSError as e:
                if e.errno == errno.EEXIST:
                    self.__logger.info(
                        "ConsignationStoreMillegrille.consigner Le fuuid %s existe deja - supprimer la source" % fuuid)
                    path_src.unlink()
                else:
                    raise e

        # Conserver dans DB et emettre evenement de consignation a la fin
        await super().consigner(path_dest, fuuid)

    async def run_entretien(self):
        pass

    async def changer_bucket(self, fuuid: str, bucket: str):
        raise NotImplementedError('todo')

    async def supprimer(self, bucket: str, fuuid: str):
        path_fichier = self.get_path_fuuid(bucket, fuuid)
        path_fichier.unlink()

    async def purger(self, fuuid: str):
        raise NotImplementedError('todo')

    async def stream_response_fuuid(
            self, fuuid: str, response: web.StreamResponse,
            start: Optional[int] = None, end: Optional[int] = None
    ):
        # Pour local FS, ignore la base de donnes. On verifie si le fichier existe dans actif ou archives
        path_fichier = self.get_path_fuuid(Constantes.BUCKET_PRINCIPAL, fuuid)
        if path_fichier.exists() is False:
            path_fichier = self.get_path_fuuid(Constantes.BUCKET_ARCHIVES, fuuid)
            if path_fichier.exists() is False:
                raise FileNotFoundError('fichier inconnu %s' % fuuid)

        with path_fichier.open(mode='rb') as input_file:
            if start is not None and start > 0:
                input_file.seek(start, 0)
                position = start
            else:
                position = 0

            while True:
                chunk = input_file.read(64*1024)
                if not chunk:
                    break

                if end is not None and position + len(chunk) > end:
                    taille_chunk = end - position + 1
                    await response.write(chunk[:taille_chunk])
                    break  # Termine
                else:
                    await response.write(chunk)

                position += len(chunk)

    async def stream_backup(self, response: web.StreamResponse, uuid_backup: str, domaine: str, fichier_nom: str):
        # Pour local FS, ignore la base de donnes. On verifie si le fichier existe dans actif ou archives
        path_backups = self.get_path_backups()
        path_fichier = pathlib.Path(path_backups, uuid_backup, domaine, fichier_nom)

        with path_fichier.open(mode='rb') as input_file:
            while True:
                chunk = input_file.read(64*1024)
                if not chunk:
                    break

                await response.write(chunk)

    async def get_fp_fuuid(self, fuuid, start: Optional[int] = None):
        # Pour local FS, ignore la base de donnes. On verifie si le fichier existe dans actif ou archives
        path_fichier = self.get_path_fuuid(Constantes.BUCKET_PRINCIPAL, fuuid)
        if path_fichier.exists() is False:
            path_fichier = self.get_path_fuuid(Constantes.BUCKET_ARCHIVES, fuuid)
            if path_fichier.exists() is False:
                raise FileNotFoundError('fichier inconnu %s' % fuuid)

        input_file = path_fichier.open(mode='rb')
        try:
            if start is not None:
                input_file.seek(start)
        except Exception as e:
            input_file.close()
            raise e

        return input_file

    async def get_fp_backup(self, uuid_backup: str, domaine: str, fichier_nom: str, start: Optional[int] = None):
        # Pour local FS, ignore la base de donnes. On verifie si le fichier existe dans actif ou archives
        path_backups = self.get_path_backups()
        path_fichier = pathlib.Path(path_backups, uuid_backup, domaine, fichier_nom)
        input_file = path_fichier.open(mode='rb')
        try:
            if start is not None:
                input_file.seek(start)
        except Exception as e:
            input_file.close()
            raise e

        return input_file

    async def visiter_bucket(self, bucket: str, path_repertoire: pathlib.Path, dao: SQLiteDetachedVisiteAppend):
        for item in path_repertoire.iterdir():
            if self._stop_store.is_set():
                raise Exception('stopping')  # Stopping

            if item.is_dir():
                # Parcourir recursivement
                await self.visiter_bucket(bucket, item, dao)
            elif item.is_file():
                stat = item.stat()

                # batch.append({'fuuid': item.name, 'taille': stat.st_size, 'bucket': bucket, 'date_presence': now})
                batch = await dao.ajouter_visite(item.name, bucket, stat.st_size)
                if batch:
                    # On a complete une batch, emettre message
                    liste_fuuids = [f['fuuid'] for f in batch]
                    await self.emettre_batch_visites(liste_fuuids, False)

    async def get_stat_fichier(self, fuuid: str, bucket: Optional[str] = None) -> Optional[dict]:
        stat = None
        if bucket is None:
            path_fichier = self.get_path_fuuid(Constantes.BUCKET_PRINCIPAL, fuuid)
            try:
                stat = path_fichier.stat()
            except FileNotFoundError:
                path_fichier = self.get_path_fuuid(Constantes.BUCKET_ARCHIVES, fuuid)
                try:
                    stat = path_fichier.stat()
                except FileNotFoundError:
                    pass
        else:
            path_fichier = self.get_path_fuuid(bucket, fuuid)
            try:
                stat = path_fichier.stat()
            except FileNotFoundError:
                pass

        if stat is None:
            return None

        info = {
            'size': stat.st_size,
            'ctime': int(stat.st_ctime),
        }

        return info

    async def verifier_fichier(self, fuuid: str, taille: int, bucket: str):
        path_fichier = self.get_path_fuuid(bucket, fuuid)
        verificateur = VerificateurHachage(fuuid)

        try:
            with open(path_fichier, 'rb') as fichier:
                while True:
                    chunk = fichier.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    verificateur.update(chunk)

            with self._etat.sqlite_connection() as connection:
                async with SQLiteWriteOperations(connection) as dao_write:
                    try:
                        verificateur.verify()
                        await dao_write.marquer_verification(fuuid, Constantes.DATABASE_ETAT_ACTIF)
                    except ErreurHachage:
                        self.__logger.error("verifier_fichier Fichier %s est corrompu, marquer manquant" % fuuid)
                        await dao_write.marquer_verification(fuuid, Constantes.DATABASE_ETAT_MANQUANT)
                        path_fichier.unlink()  # Supprimer le fichier invalide
        except FileNotFoundError:
            self.__logger.error("verifier_fichier Fichier %s est absent, marquer manquant" % fuuid)
            with self._etat.sqlite_connection() as connection:
                async with SQLiteWriteOperations(connection) as dao_write:
                    await dao_write.marquer_verification(fuuid, Constantes.DATABASE_ETAT_MANQUANT)

    async def generer_backup_sync(self):
        # await asyncio.to_thread(self.__generer_backup_sync)
        await asyncio.to_thread(self.__generer_backup_v2_sync)

    async def get_domaines_backups(self):
        path_backups = self.get_path_backups()
        liste_uuids = [f for f in path_backups.iterdir() if f.is_dir()]

        for uuid_backup in liste_uuids:
            path_backup = pathlib.Path(path_backups, uuid_backup.name)
            stat_backup = path_backup.stat()
            date_creation = int(stat_backup.st_ctime)
            for domaine in path_backup.iterdir():
                path_domaine = pathlib.Path(path_backup, domaine.name)
                fichiers = [f.name for f in path_domaine.iterdir() if f.is_file()]

                yield {
                    'uuid_backup': uuid_backup.name,
                    'domaine': domaine.name,
                    'date_creation': date_creation,
                    'fichiers': fichiers,
                }

    # def __generer_backup_sync(self):
    #     path_data = pathlib.Path(self._etat.configuration.dir_data)
    #     path_output = pathlib.Path(path_data, Constantes.FICHIER_BACKUP)
    #     path_output_work = pathlib.Path('%s.work' % path_output)
    #
    #     path_backup = self.get_path_backups()
    #
    #     with gzip.open(path_output_work, 'wt') as output:
    #         for backup_uuid_path in path_backup.iterdir():
    #             backup_uuid = backup_uuid_path.name
    #             for domaine_path in backup_uuid_path.iterdir():
    #                 domaine = domaine_path.name
    #                 for fichier_backup in domaine_path.iterdir():
    #                     # path_backup_str = '%s/%s/%s' % (backup_uuid, domaine, fichier_backup.name)
    #                     stat_fichier = fichier_backup.stat()
    #                     taille_fichier = stat_fichier.st_size
    #                     info_fichier = {
    #                         'uuid_backup': backup_uuid,
    #                         'domaine': domaine,
    #                         'nom_fichier': fichier_backup.name,
    #                         'taille': taille_fichier
    #                     }
    #                     json.dump(info_fichier, output)
    #                     output.write('\n')
    #
    #     # Renommer fichier .work
    #     path_output.unlink(missing_ok=True)
    #     path_output_work.rename(path_output)

    def __generer_backup_v2_sync(self):
        path_data = pathlib.Path(self._etat.configuration.dir_data)
        path_output = pathlib.Path(path_data, Constantes.FICHIER_BACKUP_V2)
        path_output_work = pathlib.Path('%s.work' % path_output)
        path_backup = self._path_backup_v2

        with gzip.open(path_output_work, 'wt') as output:
            for domaine_path in path_backup.iterdir():
                if domaine_path.is_dir():
                    domaine = domaine_path.name
                    try:
                        with open(pathlib.Path(domaine_path, 'courant.json')) as fp:
                            version_courante = json.load(fp)
                    except FileNotFoundError:
                        self.__logger.warning("__generer_backup_v2_sync Aucune version courante pour backup_v2 domaine %s, SKIP" % domaine)
                        continue

                    path_archives_courantes = pathlib.Path(domaine_path, 'archives', version_courante['version'])
                    fichiers_version = list()
                    header_concatene = None
                    for backup_file in path_archives_courantes.iterdir():
                        if backup_file.is_file():
                            fichiers_version.append(backup_file.name)
                            try:
                                if backup_file.name.index('_C_') > 0:
                                    with open(backup_file, 'rb') as archive:
                                        header_concatene = lire_header_archive_backup(archive)
                            except ValueError:
                                pass

                    with open(pathlib.Path(path_archives_courantes, 'info.json')) as fp:
                        info_version = json.load(fp)

                    info_domaine = {'domaine': domaine, 'info': info_version, 'concatene': header_concatene, 'fichiers': fichiers_version}

                    json.dump(info_domaine, output)
                    output.write('\n')  # Newline for record separation

        # Renommer fichier .work
        path_output.unlink(missing_ok=True)
        path_output_work.rename(path_output)

    # async def get_info_fichier_backup(self, uuid_backup: str, domaine: str, nom_fichier: str) -> dict:
    #     path_backup = pathlib.Path(self._etat.configuration.dir_consignation, Constantes.DIR_BACKUP)
    #     path_fichier = pathlib.Path(path_backup, uuid_backup, domaine, nom_fichier)
    #
    #     info = path_fichier.stat()
    #
    #     return {
    #         'taille': info.st_size
    #     }

    # async def upload_backups_primaire(self, connection_transfert: SQLiteConnection, session: ClientSession):
    #     path_backup = pathlib.Path(self._etat.configuration.dir_consignation, Constantes.DIR_BACKUP)
    #     async with SQLiteTransfertOperations(connection_transfert) as transfert_dao:
    #         for path_uuid_backup in path_backup.iterdir():
    #             uuid_backup = path_uuid_backup.name
    #             for path_domaine in path_uuid_backup.iterdir():
    #                 domaine = path_domaine.name
    #                 for path_fichier in path_domaine.iterdir():
    #                     nom_fichier = path_fichier.name
    #
    #                     info = await asyncio.to_thread(
    #                         transfert_dao.get_info_backup_primaire, uuid_backup, domaine, nom_fichier)
    #
    #                     if info is None:
    #                         self.__logger.info("Fichier backup %s/%s/%s absent du primaire, on upload" % (uuid_backup, domaine, nom_fichier))
    #                         with path_fichier.open('rb') as fichier:
    #                             await self.upload_backup_primaire(session, uuid_backup, domaine, nom_fichier, fichier)

    # Backup V2

    async def sync_backups_v2_primaire(self, session: ClientSession, url_backup: str, ssl_context: SSLContext):
        path_backup_lock = pathlib.Path(self._path_backup_v2, 'sync.lock')
        try:
            with open(path_backup_lock, 'w') as lock_file:
                try:
                    await sync_backups_v2_primaire(self._path_backup_v2, session, ssl_context, url_backup)
                except:
                    self.__logger.exception("sync_backups_v2_primaire Erreur sync backup files avec primaire")
        finally:
            path_backup_lock.unlink(missing_ok=True)

    async def get_backup_v2_versions(self, domaine: str) -> dict:
        path_domaine = pathlib.Path(self._path_backup_v2, domaine)
        path_archives = pathlib.Path(path_domaine, 'archives')
        path_info_courante = pathlib.Path(path_domaine, 'courant.json')
        versions = list()
        courante = None
        try:
            with open(path_info_courante, 'rt') as fichier:
                courante = json.load(fichier)
            for path_uuid_backup in path_archives.iterdir():
                if path_uuid_backup.is_dir():
                    path_info = pathlib.Path(path_archives, path_uuid_backup, 'info.json')
                    try:
                        with open(path_info, 'rt') as fichier:
                            info_version = json.load(fichier)
                        versions.append(info_version)
                    except FileNotFoundError:
                        self.__logger.info("Version backup %s : aucun fichier info.json" % path_info)
                    except json.JSONDecodeError:
                        self.__logger.warning("Version backup %s : info.json corrompu" % path_info)
        except FileNotFoundError:
            # Le fichier courant.json ou le repertoire d'archives n'existe pas encore
            pass  # Le domaine est nouveau ou le serveur de consignation a ete resette

        return { "versions": versions, "version_courante": courante }

    async def get_backup_v2_list(self, domaine, version: Optional[str] = None) -> list[str]:
        if version is None:
            # Lister les archives finales du domaine
            path_archives = pathlib.Path(self._path_backup_v2, domaine, "final")
        else:
            path_archives = pathlib.Path(self._path_backup_v2, domaine, "archives", version)

        fichiers = list()
        try:
            for fichier in path_archives.iterdir():
                if fichier.is_file() and fichier.suffix == ".mgbak":
                    fichiers.append(fichier.name)
        except FileNotFoundError:
            pass  # Le repertoire n'existe pas, aucuns fichiers a ajouter

        return fichiers

    async def put_backup_v2_fichier(self, fichier_temp: tempfile.TemporaryFile,
                                    domaine: str, nom_fichier: str, type_fichier: str, version: Optional[str] = None):
        if version is None:
            # Lister les archives finales du domaine
            path_archives = pathlib.Path(self._path_backup_v2, domaine, "final")
        else:
            path_archives = pathlib.Path(self._path_backup_v2, domaine, "archives", version)

        # Copier le contenu du fichier temporaire vers le repertoire destination
        fichier_temp.seek(0)
        header_fichier = lire_header_archive_backup(fichier_temp)
        fichier_temp.seek(0)

        # S'assurer que le fichier de backup est pour le system courant (idmg)
        idmg = self._etat.clecertificat.enveloppe.idmg
        if header_fichier['idmg'] != idmg:
            self.__logger.error("put_backup_v2_fichier Fichier upload pour le mauvais IDMG")
            return web.HTTPExpectationFailed()

        path_fichier = pathlib.Path(path_archives, nom_fichier)

        if header_fichier['type_archive'] in ['C', 'F'] or version == 'NEW':
            # S'assurer que le repertoire existe
            path_archives.mkdir(parents=True, exist_ok=True)

        await asyncio.to_thread(copier_contenu_fichier_safe, fichier_temp, path_fichier)

        if header_fichier['type_archive'] == 'C':
            # Nouveau fichier concatene, on met a jour la version courante
            # info_version = {'version': version, 'date': int(datetime.datetime.now(datetime.UTC).timestamp())}
            fin_backup_secs = math.floor(header_fichier['fin_backup'] / 1000)
            info_version = {'version': version, 'date': fin_backup_secs}
            path_fichier_info = pathlib.Path(path_archives, 'info.json')
            with open(path_fichier_info, 'wt') as fichier:
                json.dump(info_version, fichier)

            # Remplacer le fichier courant.json
            path_fichier_courant = pathlib.Path(self._path_backup_v2, domaine, 'courant.json')
            path_fichier_courant.unlink(missing_ok=True)
            # try:
            #     path_fichier_info.symlink_to(path_fichier_courant)
            # except NotImplementedError:
            with open(path_fichier_info, 'rb') as src:
                await asyncio.to_thread(copier_contenu_fichier_safe, src, path_fichier_courant)

    async def get_backup_v2_fichier_stream(self, domaine: str, nom_fichier: str,
                                           version: Optional[str] = None) -> BinaryIO:
        if version is None:
            # Lister les archives finales du domaine
            path_archives = pathlib.Path(self._path_backup_v2, domaine, "final")
        else:
            path_archives = pathlib.Path(self._path_backup_v2, domaine, "archives", version)

        path_fichier = pathlib.Path(path_archives, nom_fichier)
        if path_fichier.exists() is False:
            raise FileNotFoundError()

        fichier = open(path_fichier, 'rb')
        return fichier

    async def get_backup_v2_domaines(self, domaines: Optional[list[str]] = None, courant=True, stats=False, cles=False):
        domaines_reponse = []

        for rep_domaine in self._path_backup_v2.iterdir():
            nom_domaine = rep_domaine.name
            if domaines is not None:
                if nom_domaine not in domaines:
                    continue  # Skip, ce domaine n'a pas ete demande

            if rep_domaine.is_dir():
                self.__logger.debug("Domaine %s" % nom_domaine)
                domaine = {'domaine': nom_domaine}
                domaines_reponse.append(domaine)

                path_courant = pathlib.Path(rep_domaine, 'courant.json')
                try:
                    with open(path_courant, 'rt') as fichier:
                        info_courant = json.load(fichier)
                except FileNotFoundError:
                    # Pas d'information sur le backup courant
                    info_courant = {'version': 'NEW'}

                domaine['concatene'] = info_courant

                if courant is not True:
                    raise NotImplementedError('todo')

                if stats is not False or cles is not False:
                    version_courante = info_courant['version']
                    path_version = pathlib.Path(rep_domaine, 'archives', version_courante)
                    # Parcourir tous les fichiers de la version
                    compteur_transactions = 0
                    date_plus_recent = 0
                    cles_dict = {}
                    for archive_backup in path_version.iterdir():
                        if archive_backup.name.endswith('.mgbak'):
                            # Charger le header
                            with open(archive_backup, 'rb') as fichier:
                                header = lire_header_archive_backup(fichier)
                            compteur_transactions += header['nombre_transactions']
                            if header['fin_backup'] > date_plus_recent:
                                date_plus_recent = header['fin_backup']
                            cle_id = header['cle_id']
                            if cle_id not in cles_dict:
                                cles_dict[cle_id] = header['cle_dechiffrage']

                    if stats:
                        domaine['nombre_transactions'] = compteur_transactions
                        domaine['transaction_plus_recente'] = floor(date_plus_recent / 1000)  # To epoch seconds

                    if cles:
                        domaine['cles'] = cles_dict

        return domaines_reponse

    async def rotation_backups_v2(self, nombre_archives=3):
        await rotation_backups_v2(self._path_backup_v2, nombre_archives)

def map_type(type_store: str) -> Type[ConsignationStore]:
    if type_store == Constantes.TYPE_STORE_MILLEGRILLE:
        return ConsignationStoreMillegrille
    elif type_store == Constantes.TYPE_STORE_SFTP:
        raise NotImplementedError()
    elif type_store == Constantes.TYPE_STORE_AWSS3:
        raise NotImplementedError()
    else:
        raise Exception('Type %s non supporte' % type_store)


def copier_contenu_fichier_safe(src: BufferedReader, dest: pathlib.Path):
    path_fichier_work = pathlib.Path(dest.parent, dest.name + '.work')
    path_fichier_work.unlink(missing_ok=True)
    with open(path_fichier_work, 'wb') as fichier:
        while True:
            chunk = src.read(64 * 1024)
            if not chunk:
                break
            fichier.write(chunk)

    # Renommer le fichier d'archive (retrirer .work)
    path_fichier_work.rename(dest)
