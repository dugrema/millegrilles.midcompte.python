import asyncio
import datetime
import errno
import gzip
import json
import logging
import pathlib
import shutil
import sqlite3
import tempfile

from aiohttp import web, ClientSession
from typing import Optional, Type, Union

import pytz

from millegrilles_fichiers.SQLiteDao import SQLiteConnection
from millegrilles_messages.messages import Constantes as ConstantesMillegrilles
from millegrilles_messages.messages.Hachage import VerificateurHachage, ErreurHachage

import millegrilles_fichiers.DatabaseScripts as scripts_database
from millegrilles_fichiers import Constantes
from millegrilles_fichiers.EtatFichiers import EtatFichiers
from millegrilles_fichiers.UploadFichiersPrimaire import EtatUpload, feed_filepart2
from millegrilles_fichiers.SQLiteDao import (SQLiteReadOperations, SQLiteWriteOperations, SQLiteBatchOperations,
                                             SQLiteDetachedReclamationAppend, SQLiteDetachedVisiteAppend)


class ConsignationStore:

    def __init__(self, etat: EtatFichiers):
        self._etat = etat
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

        # Creer repertoire data, path database
        path_database = pathlib.Path(
            self._etat.configuration.dir_consignation, Constantes.DIR_DATA, Constantes.FICHIER_DATABASE_FICHIERS)
        self.__path_database = path_database

        self._stop_store: Optional[asyncio.Event] = None

    def initialiser(self):
        self._stop_store = asyncio.Event()

        path_backup = pathlib.Path(
            self._etat.configuration.dir_consignation, Constantes.DIR_BACKUP)
        path_backup.mkdir(parents=True, exist_ok=True)

        self.__path_database.parent.mkdir(parents=True, exist_ok=True)
        with self._etat.sqlite_connection() as connection:
            connection.init_database()

    async def run_entretien(self):
        raise NotImplementedError('must override')

    async def stop(self):
        self._stop_store.set()

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

        # # batch, resultat = await asyncio.to_thread(entretien_db.commit_visites)
        # batch, resultat = await dao.commit_batch()
        # if batch is not None:
        #     await self.emettre_batch_visites(batch, False)
        #
        # # Marquer tous les fichiers 'manquants' qui viennent d'etre visites comme actifs (utilise date debut)
        # await asyncio.to_thread(dao.marquer_actifs_visites)

        self.__logger.info("visiter_fuuids Fin")

    async def visiter_bucket(self, bucket: str, path_repertoire: pathlib.Path, dao: SQLiteDetachedVisiteAppend):
        """ Visiter tous les fichiers presents, s'assurer qu'ils sont dans la base de donnees. """
        raise NotImplementedError('must override')

    async def get_info_fichier_backup(self, uuid_backup: str, domaine: str, nom_fichier: str) -> dict:
        """ Retourne l'information d'un fichier de backup ou FileNotFoundError si non trouve. """
        raise NotImplementedError('must override')

    async def verifier_fuuids(self, limite=Constantes.CONST_LIMITE_TAILLE_VERIFICATION):
        await self.__verifier_fuuids(limite)

    async def __verifier_fuuids(self, limite=Constantes.CONST_LIMITE_TAILLE_VERIFICATION):
        """ Visiter tous les fichiers presents, s'assurer qu'ils sont dans la base de donnees. """
        with self._etat.sqlite_connection() as connection:
            async with SQLiteReadOperations(connection) as dao_read:
                liste_fichiers = await asyncio.to_thread(dao_read.charger_verifier_fuuids, limite)

            async with SQLiteBatchOperations(connection) as dao:
                # Verifier chaque fichier individuellement
                # On conserve le lock sur operations de batch pour la duree du traitement
                for fichier in liste_fichiers:
                    await self.verifier_fichier(dao, **fichier)

    async def supprimer_orphelins(self):
        with self._etat.sqlite_connection() as connection:
            # Ouvrir l'operation de batch (lock)
            async with SQLiteBatchOperations(connection) as dao:
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
                async with SQLiteReadOperations(connection) as dao_read:
                    batch_orphelins = await asyncio.to_thread(dao_read.identifier_orphelins, expiration)

                fuuids = [f['fuuid'] for f in batch_orphelins]
                fuuids_a_supprimer = set(fuuids)
                if est_primaire:
                    # Transmettre commande pour confirmer que la suppression peut avoir lieu
                    commande = {'fuuids': fuuids}
                    reponse = await producer.executer_commande(
                        commande,
                        domaine=Constantes.DOMAINE_GROSFICHIERS, action=Constantes.COMMANDE_SUPPRIMER_ORPHELINS,
                        exchange=ConstantesMillegrilles.SECURITE_PRIVE, timeout=30
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
                            # await asyncio.to_thread(entretien_db.supprimer, fuuid)
                            await dao.supprimer(fuuid)

        pass

    async def verifier_fichier(self, batch_dao: SQLiteBatchOperations, fuuid: str, taille: int, bucket: str):
        """ Verifie un fichier individuellement. Marque resultat dans la base de donnees. """
        raise NotImplementedError('must override')

    async def conserver_backup(self, fichier_temp: tempfile.TemporaryFile, uuid_backup: str, domaine: str,
                               nom_fichier: str):
        raise NotImplementedError('must override')

    async def rotation_backups(self, uuid_backups_conserver: list[str]):
        """ Supprime tous les backups qui ne sont pas dans la liste """
        raise NotImplementedError('must override')

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
        # con = self.ouvrir_database()
        # cur = con.cursor()

        # dict_fuuids = dict()
        # idx = 0
        # for fuuid in fuuids:
        #     dict_fuuids['f%d' % idx] = fuuid
        #     idx += 1

        date_reclamation = datetime.datetime.now(tz=pytz.UTC)
        # params = {
        #     'date_reclamation': datetime.datetime.now(tz=pytz.UTC)
        # }
        # params.update(dict_fuuids)

        # requete = scripts_database.UPDATE_ACTIVER_SI_ORPHELIN.replace('$fuuids', ','.join([':%s' % f for f in dict_fuuids.keys()]))
        # cur.execute(requete, params)
        # con.commit()

        with self._etat.sqlite_connection() as connection:
            async with SQLiteWriteOperations(connection) as dao_write:
                fuuid_keys = await asyncio.to_thread(dao_write.activer_si_orphelin, fuuids, date_reclamation)

            async with SQLiteReadOperations(connection) as dao_read:
                liste_fichiers_actifs = await asyncio.to_thread(dao_read.get_info_fichiers_actif, fuuid_keys)

        # liste_fichiers_actifs = list()
        # requete = scripts_database.SELECT_INFO_FICHIERS_ACTIFS.replace('$fuuids', ','.join([':%s' % f for f in dict_fuuids.keys()]))
        # cur.execute(requete, dict_fuuids)

        # while True:
        #     row = cur.fetchone()
        #     if row is None:
        #         break
        #     fuuid = row[0]
        #     liste_fichiers_actifs.append(fuuid)
        #
        # cur.close()
        # con.close()

        return liste_fichiers_actifs

    # def ouvrir_database(self, ro=False) -> sqlite3.Connection:
    #     # return sqlite3.connect(self.__path_database, check_same_thread=True)
    #     raise NotImplementedError('obsolete')

    async def get_stats(self):
        with self._etat.sqlite_connection() as connection:
            async with SQLiteReadOperations(connection) as dao_read:
                return await asyncio.to_thread(dao_read.get_stats_fichiers)

        # con = self.ouvrir_database()
        # cur = con.cursor()
        # cur.execute(scripts_database.SELECT_STATS_FICHIERS)
        #
        # resultats_dict = dict()
        # nombre_orphelins = 0
        # taille_orphelins = 0
        # nombre_manquants = 0
        #
        # while True:
        #     row = cur.fetchone()
        #     if row is None:
        #         break
        #
        #     etat_fichier, bucket, nombre, taille = row
        #     if etat_fichier == Constantes.DATABASE_ETAT_ACTIF:
        #         resultats_dict[bucket] = {
        #             'nombre': nombre,
        #             'taille': taille
        #         }
        #     elif etat_fichier == Constantes.DATABASE_ETAT_ORPHELIN:
        #         nombre_orphelins += nombre
        #         taille_orphelins += taille
        #     elif etat_fichier == Constantes.DATABASE_ETAT_MANQUANT:
        #         nombre_manquants += nombre
        #
        # cur.close()
        # con.close()
        #
        # resultats_dict[Constantes.DATABASE_ETAT_ORPHELIN] = {
        #     'nombre': nombre_orphelins,
        #     'taille': taille_orphelins
        # }
        #
        # resultats_dict[Constantes.DATABASE_ETAT_MANQUANT] = {
        #     'nombre': nombre_manquants,
        # }
        #
        # return resultats_dict

    async def get_info_fichier(self, fuuid: str) -> Optional[dict]:
        with self._etat.sqlite_connection() as connection:
            async with SQLiteReadOperations(connection) as dao_read:
                return await asyncio.to_thread(dao_read.get_info_fichier, fuuid)

        # cur.execute(scripts_database.SELECT_INFO_FICHIER, {'fuuid': fuuid})
        # row = cur.fetchone()
        # if row is not None:
        #     _fuuid, etat_fichier, taille, bucket, date_presence, date_verification, date_reclamation = row
        #     cur.close()
        #     con.close()
        #
        #     return {
        #         'fuuid': fuuid,
        #         'taille': taille,
        #         'etat_fichier': etat_fichier,
        #         'date_presence': date_presence,
        #         'date_verification': date_verification,
        #         'date_reclamation': date_reclamation
        #     }
        # else:
        #     return None

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
        with SQLiteConnection(path_db_sync, check_same_thread=False) as connection:
            async with SQLiteDetachedReclamationAppend(connection) as detached_dao:
                await detached_dao.reclamer_fuuids(fuuids, bucket)

    # async def marquer_orphelins(self, dao_batch: SQLiteBatchOperations, debut_reclamation: datetime.datetime, complet=False):
    #     if complet:
    #         # Marquer les fichiers avec vieille date de reclamation comme non reclames (orphelins)
    #         resultat = await asyncio.to_thread(dao_batch.marquer_orphelins, debut_reclamation)
    #         await dao_batch.commit_batch()
    #         self.__logger.info("__marquer_orphelins Marquer actif -> orphelins : %d rows" % resultat.rowcount)
    #     else:
    #         self.__logger.info("__marquer_orphelins Skip, reclamation est incomplete")
    #
    #     # Marquer fichiers orphelins qui viennent d'etre reclames comme actif
    #     # resultat = cur.execute(scripts_database.UPDATE_MARQUER_ACTIF, {'date_reclamation': debut_reclamation})
    #     resultat = await asyncio.to_thread(dao_batch.marquer_actifs, debut_reclamation)
    #     await dao_batch.commit_batch()
    #     self.__logger.info("__marquer_orphelins Marquer orphelins -> actif : %d rows" % resultat.rowcount)

    async def generer_reclamations_sync(self, connection: SQLiteConnection):
        dir_data = pathlib.Path(self._etat.configuration.dir_consignation)
        fichier_reclamations = pathlib.Path(dir_data,
                                            Constantes.DIR_DATA, Constantes.FICHIER_RECLAMATIONS_PRIMAIRES)

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

    async def upload_backups_primaire(self, session: ClientSession, dao: SQLiteReadOperations):
        raise NotImplementedError('must implement')

    async def upload_backup_primaire(self, session: ClientSession, uuid_backup: str, domaine: str, nom_fichier: str, fichier):
        url_consignation_primaire = await self._etat.charger_consignation_primaire()
        url_backup = '%s/fichiers_transfert/backup/upload' % url_consignation_primaire
        url_fichier = f"{url_backup}/{uuid_backup}/{domaine}/{nom_fichier}"

        etat_upload = EtatUpload('', fichier, self._stop_store, 0)

        feeder = feed_filepart2(etat_upload, limit=10_000_000)
        session_coro = session.put(url_fichier, ssl=self._etat.ssl_context, data=feeder)

        # Uploader chunk
        session_response = None
        try:
            session_response = await session_coro
            if etat_upload.done is False:
                raise Exception('Erreur upload fichier backup - feeder not done')
        finally:
            if session_response is not None:
                session_response.release()
                session_response.raise_for_status()

    async def get_domaines_backups(self):
        """ @:return Generateur par domaine des uuid de backups """
        raise NotImplementedError('must implement')


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
        await super().consigner(path_src, fuuid)

        path_dest = self.get_path_fuuid(Constantes.BUCKET_PRINCIPAL, fuuid)
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

    # async def visiter_fuuids(self):
    #     dir_buckets = pathlib.Path(self._etat.configuration.dir_consignation, Constantes.DIR_BUCKETS)
    #     self.__logger.info("visiter_fuuids Debut avec path buckets %s" % dir_buckets)
    #     with EntretienDatabase(self._etat, check_same_thread=False) as entretien_db:
    #         # Parcourir tous les buckets recursivement (depth-first)
    #         for bucket in dir_buckets.iterdir():
    #             self.__logger.debug("Visiter bucket %s" % bucket.name)
    #             await self.visiter_bucket(bucket.name, bucket, entretien_db)
    #
    #         batch, resultat = await asyncio.to_thread(entretien_db.commit_visites)
    #         await self.emettre_batch_visites(batch, False)
    #
    #         # Marquer tous les fichiers 'manquants' qui viennent d'etre visites comme actifs (utilise date debut)
    #         await asyncio.to_thread(entretien_db.marquer_actifs_visites)
    #
    #     self.__logger.info("visiter_fuuids Fin")

    async def visiter_bucket(self, bucket: str, path_repertoire: pathlib.Path, dao: SQLiteDetachedVisiteAppend):

        batch = list()
        now = datetime.datetime.now(tz=pytz.UTC)

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

                # if len(batch) >= Constantes.CONST_BATCH_VISITES:
                #     batch = await dao.ajouter_visite(item.name, bucket, stat.st_size)
                #     await self.traiter_batch_visites(dao, batch)
                #     # # batch, resultat = await asyncio.to_thread(entretien_db.commit_visites)
                #     # debut_commit = datetime.datetime.utcnow()
                #     # # batch, resultat = await dao.commit_batch()
                #     # resultat = await dao.ajouter_visites(batch)
                #     # duree = datetime.datetime.utcnow() - debut_commit
                #     # self.__logger.info("visiter_bucket Commit batch visite complete (duree : %s)" % duree)
                #     # liste_fuuids = [f['fuuid'] for f in batch]
                #     # await self.emettre_batch_visites(liste_fuuids, False)
                #     # # try:
                #     # #     await asyncio.wait_for(self._stop_store.wait(), timeout=Constantes.CONST_ATTENTE_ENTRE_BATCH_VISITES)
                #     # # except asyncio.TimeoutError:
                #     # #     pass  # OK

    # async def traiter_batch_visites(self, dao: SQLiteDetachedVisiteAppend, batch: list):
    #     self.__logger.info("visiter_bucket Commit batch visite (%d)" % len(batch))
    #     # batch, resultat = await asyncio.to_thread(entretien_db.commit_visites)
    #     debut_commit = datetime.datetime.utcnow()
    #     # batch, resultat = await dao.commit_batch()
    #     resultat = await dao.ajouter_visites(batch)
    #     duree = datetime.datetime.utcnow() - debut_commit
    #     self.__logger.info("visiter_bucket Commit batch visite complete (duree : %s)" % duree)
    #     liste_fuuids = [f['fuuid'] for f in batch]
    #     await self.emettre_batch_visites(liste_fuuids, False)

    async def conserver_backup(self, fichier_temp: tempfile.TemporaryFile, uuid_backup: str, domaine: str,
                               nom_fichier: str):
        path_backup = pathlib.Path(self._etat.configuration.dir_consignation, Constantes.DIR_BACKUP, uuid_backup, domaine)
        # Utiliser thread separee pour processus de copie (blocking)
        await asyncio.to_thread(self.__conserver_backup, fichier_temp, path_backup, nom_fichier)

    def __conserver_backup(self, fichier_temp: tempfile.TemporaryFile, repertoire: pathlib.Path, nom_fichier):
        fichier_temp.seek(0)
        path_fichier_backup = pathlib.Path(repertoire, nom_fichier)
        path_fichier_work = pathlib.Path(repertoire, '%s.work' % nom_fichier)
        repertoire.mkdir(parents=True, exist_ok=True)
        with open(path_fichier_work, 'wb') as fichier:
            while True:
                chunk = fichier_temp.read(64*1024)
                if not chunk:
                    break
                fichier.write(chunk)

        # Renommer fichier (retrirer .work)
        path_fichier_work.rename(path_fichier_backup)

    async def rotation_backups(self, uuid_backups_conserver: list[str]):
        """ Supprime tous les backups qui ne sont pas dans la liste """
        dir_consignation = pathlib.Path(self._etat.configuration.dir_consignation, Constantes.DIR_BACKUP)
        for item in dir_consignation.iterdir():
            if item.is_dir():
                if item.name not in uuid_backups_conserver:
                    self.__logger.info("Supprimer repertoire de backup %s" % item.name)
                    shutil.rmtree(item)

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

    async def verifier_fichier(self, batch_dao: SQLiteBatchOperations, fuuid: str, taille: int, bucket: str):
        path_fichier = self.get_path_fuuid(bucket, fuuid)
        verificateur = VerificateurHachage(fuuid)

        try:
            with open(path_fichier, 'rb') as fichier:
                while True:
                    chunk = fichier.read(64*1024)
                    if not chunk:
                        break
                    verificateur.update(chunk)
            try:
                verificateur.verify()
                await batch_dao.marquer_verification(fuuid, Constantes.DATABASE_ETAT_ACTIF)
            except ErreurHachage:
                self.__logger.error("verifier_fichier Fichier %s est corrompu, marquer manquant" % fuuid)
                await batch_dao.marquer_verification(fuuid, Constantes.DATABASE_ETAT_MANQUANT)
                path_fichier.unlink()  # Supprimer le fichier invalide
        except FileNotFoundError:
            self.__logger.error("verifier_fichier Fichier %s est absent, marquer manquant" % fuuid)
            await batch_dao.marquer_verification(fuuid, Constantes.DATABASE_ETAT_MANQUANT)

    async def generer_backup_sync(self):
        await asyncio.to_thread(self.__generer_backup_sync)

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

    def __generer_backup_sync(self):
        path_data = pathlib.Path(self._etat.configuration.dir_consignation, Constantes.DIR_DATA)
        path_output = pathlib.Path(path_data, Constantes.FICHIER_BACKUP)
        path_output_work = pathlib.Path('%s.work' % path_output)

        path_backup = self.get_path_backups()

        with gzip.open(path_output_work, 'wt') as output:
            for backup_uuid_path in path_backup.iterdir():
                backup_uuid = backup_uuid_path.name
                for domaine_path in backup_uuid_path.iterdir():
                    domaine = domaine_path.name
                    for fichier_backup in domaine_path.iterdir():
                        # path_backup_str = '%s/%s/%s' % (backup_uuid, domaine, fichier_backup.name)
                        stat_fichier = fichier_backup.stat()
                        taille_fichier = stat_fichier.st_size
                        info_fichier = {
                            'uuid_backup': backup_uuid,
                            'domaine': domaine,
                            'nom_fichier': fichier_backup.name,
                            'taille': taille_fichier
                        }
                        json.dump(info_fichier, output)
                        output.write('\n')

        # Renommer fichier .work
        path_output.unlink(missing_ok=True)
        path_output_work.rename(path_output)

    async def get_info_fichier_backup(self, uuid_backup: str, domaine: str, nom_fichier: str) -> dict:
        path_backup = pathlib.Path(self._etat.configuration.dir_consignation, Constantes.DIR_BACKUP)
        path_fichier = pathlib.Path(path_backup, uuid_backup, domaine, nom_fichier)

        info = path_fichier.stat()

        return {
            'taille': info.st_size
        }

    async def upload_backups_primaire(self, session: ClientSession, dao: SQLiteReadOperations):
        path_backup = pathlib.Path(self._etat.configuration.dir_consignation, Constantes.DIR_BACKUP)
        for path_uuid_backup in path_backup.iterdir():
            uuid_backup = path_uuid_backup.name
            for path_domaine in path_uuid_backup.iterdir():
                domaine = path_domaine.name
                for path_fichier in path_domaine.iterdir():
                    nom_fichier = path_fichier.name

                    info = await asyncio.to_thread(
                        dao.get_info_backup_primaire, uuid_backup, domaine, nom_fichier)

                    if info is None:
                        self.__logger.info("Fichier backup %s/%s/%s absent du primaire, on upload" % (uuid_backup, domaine, nom_fichier))
                        with path_fichier.open('rb') as fichier:
                            await self.upload_backup_primaire(session, uuid_backup, domaine, nom_fichier, fichier)


def map_type(type_store: str) -> Type[ConsignationStore]:
    if type_store == Constantes.TYPE_STORE_MILLEGRILLE:
        return ConsignationStoreMillegrille
    elif type_store == Constantes.TYPE_STORE_SFTP:
        raise NotImplementedError()
    elif type_store == Constantes.TYPE_STORE_AWSS3:
        raise NotImplementedError()
    else:
        raise Exception('Type %s non supporte' % type_store)
