import asyncio
import datetime
import json
import logging
import pathlib
import sqlite3
from typing import Optional

import pytz

from millegrilles_fichiers import Constantes, DatabaseScripts as scripts_database

sqlite3.threadsafety = 2


class SQLiteLocks:

    def __init__(self):
        self.__write_lock: Optional[asyncio.BoundedSemaphore] = None
        self.__batch_job_lock: Optional[asyncio.BoundedSemaphore] = None

    async def ainit(self):
        self.__write_lock = asyncio.BoundedSemaphore(value=1)

        # Lock special conserve pour la duree d'une job. Faire un acquire avant write.
        self.__batch_job_lock = asyncio.BoundedSemaphore(value=1)

    @property
    def write(self):
        return self.__write_lock

    @property
    def batch_job(self):
        return self.__batch_job_lock


class SQLiteConnection:

    def __init__(self, path_data: pathlib.Path, locks: Optional[SQLiteLocks] = None, check_same_thread=True, timeout=5.0):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__path_data = path_data
        self.__path_database = pathlib.Path(path_data, Constantes.FICHIER_DATABASE)
        self.__sqlite_locks = locks
        self.__check_same_thread = check_same_thread
        self.__timeout = timeout

        self.__con: Optional[sqlite3.Connection] = None

    async def ainit(self):
        if self.__sqlite_locks is None:
            sqlite_locks = SQLiteLocks()
            await sqlite_locks.ainit()
            self.__sqlite_locks = sqlite_locks

    def open(self):
        self.__con = sqlite3.connect(
            self.__path_database,
            timeout=self.__timeout, check_same_thread=self.__check_same_thread
        )

    def close(self):
        self.__con.commit()
        self.__con.close()
        self.__con = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def cursor(self) -> sqlite3.Cursor:
        return self.__con.cursor()

    def commit(self):
        return self.__con.commit()

    @property
    def locks(self) -> SQLiteLocks:
        return self.__sqlite_locks

    @property
    def path_database(self) -> pathlib.Path:
        return self.__path_database

    @property
    def path_data(self) -> pathlib.Path:
        return self.__path_data


class SQLiteCursor:

    def __init__(self, connection: SQLiteConnection):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self._connection = connection
        self._cur: Optional[sqlite3.Cursor] = None

    def open(self):
        self._cur = self._connection.cursor()

    def close(self):
        self._connection.commit()
        self._cur.close()
        self._cur = None

    async def __aenter__(self):
        self.open()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def reset_intermediaires(self):
        path_intermediaires = self.get_path_relamations_intermediaires()
        path_intermediaires.unlink(missing_ok=True)

    def get_path_database(self):
        return self._connection.path_database

    def get_path_relamations_intermediaires(self):
        return pathlib.Path(self._connection.path_data, Constantes.FICHIER_RECLAMATIONS_INTERMEDIAIRES)


class SQLiteReadOperations(SQLiteCursor):

    def __init__(self, connection: SQLiteConnection):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        super().__init__(connection)

    def identifier_orphelins(self, expiration: datetime.datetime) -> list:
        params = {
            'date_reclamation': expiration,
            'limit': 1000,
        }
        orphelins = list()
        self._cur.execute(scripts_database.SELECT_BATCH_ORPHELINS, params)
        while True:
            row = self._cur.fetchone()
            if row is None:
                break
            fuuid, taille, bucket_visite = row
            orphelins.append({'fuuid': fuuid, 'taille': taille, 'bucket': bucket_visite})

        return orphelins

    def generer_relamations_primaires(self, fp):
        """ Genere le contenu du fichier de transfert d'etat fichiers.jsonl.gz """
        self._cur.execute(scripts_database.SELECT_FICHIERS_TRANSFERT)
        while True:
            row = self._cur.fetchone()
            if row is None:
                break
            fuuid, etat_fichier, taille, bucket_visite = row
            contenu_ligne = {'fuuid': fuuid, 'etat_fichier': etat_fichier, 'taille': taille, 'bucket': bucket_visite}
            json.dump(contenu_ligne, fp)
            fp.write('\n')

        self.reset_intermediaires()

    def get_etat_uploads(self):
        self._cur.execute(scripts_database.SELECT_ETAT_UPLOADS)
        row = self._cur.fetchone()
        if row:
            nombre, taille = row
            return {'nombre': nombre, 'taille': taille}

        return None

    def get_etat_downloads(self):
        self._cur.execute(scripts_database.SELECT_ETAT_DOWNLOADS)
        row = self._cur.fetchone()
        if row:
            nombre, taille = row
            return {'nombre': nombre, 'taille': taille}

        return None

    def get_info_backup_primaire(self, uuid_backup: str, domaine: str, nom_fichier: str) -> Optional[dict]:
        params = {'uuid_backup': uuid_backup, 'domaine': domaine, 'nom_fichier': nom_fichier}

        self._cur.execute(scripts_database.SELECT_BACKUP_PRIMAIRE, params)
        row = self._cur.fetchone()
        if row is not None:
            uuid_backup, domaine, nom_fichier, taille = row
            return {
                'uuid_backup': uuid_backup,
                'domaine': domaine,
                'nom_fichier': nom_fichier,
                'taille': taille
            }

        return None


class SQLiteWriteOperations(SQLiteCursor):

    def __init__(self, connection: SQLiteConnection):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        super().__init__(connection)

        self.__write_lock = connection.locks.write

    async def __aenter__(self):
        await self.__write_lock.acquire()
        self.open()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()
        self.__write_lock.release()
        return False

    def supprimer(self, fuuid: str):
        params = {'fuuid': fuuid}
        self._cur.execute(scripts_database.DELETE_SUPPRIMER_FUUIDS, params)

    def consigner(self, fuuid: str, taille: int, bucket: str):
        date_now = datetime.datetime.now(tz=pytz.UTC)
        data = {
            'fuuid': fuuid,
            'etat_fichier': Constantes.DATABASE_ETAT_ACTIF,
            'taille': taille,
            'bucket': bucket,
            'date_presence': date_now,
            'date_verification': date_now,
            'date_reclamation': date_now,
        }

        nouveau = False

        try:
            self._cur.execute(scripts_database.INSERT_FICHIER, data)
            nouveau = True
        except sqlite3.IntegrityError as e:
            if 'FICHIERS.fuuid' in e.args[0]:
                self.__logger.debug("ConsignationStore.consigner fuuid %s existe deja - OK" % fuuid)
                resultat_update_manquant = self._cur.execute(scripts_database.UPDATE_ACTIVER_SI_MANQUANT, data)
                if resultat_update_manquant.rowcount > 0:
                    # Le fichier etait manquant, on le considere nouveau
                    nouveau = True
                self._cur.execute(scripts_database.UPDATE_VERIFIER_FICHIER, data)
            else:
                raise e

        if nouveau:
            try:
                data_intermediaire = {
                    'fuuid': fuuid,
                    'etat_fichier': Constantes.DATABASE_ETAT_ACTIF,
                    'taille': taille,
                    'bucket': bucket,
                }
                path_intermediaire = self.get_path_relamations_intermediaires()
                with open(path_intermediaire, 'at') as fichier:
                    json.dump(data_intermediaire, fichier)
                    fichier.write('\n')
            except Exception:
                self.__logger.exception("Erreur sauvegarde fuuid dans liste intermediaire")

    def truncate_fichiers_primaire(self):
        self._cur.execute(scripts_database.DELETE_TRUNCATE_FICHIERS_PRIMAIRE)

    def truncate_backup_primaire(self):
        self._cur.execute(scripts_database.DELETE_TRUNCATE_BACKUPS_PRIMAIRE)

    def get_next_download(self):
        params = {'date_activite': datetime.datetime.now(tz=pytz.UTC)}
        row = None
        try:
            self._cur.execute(scripts_database.UPDATE_GET_NEXT_DOWNLOAD, params)
            row = self._cur.fetchone()
        finally:
            self._cur.close()

        if row is not None:
            fuuid, taille = row
            return {'fuuid': fuuid, 'taille': taille}

        return None

    def get_next_upload(self):
        params = {'date_activite': datetime.datetime.now(tz=pytz.UTC)}
        row = None
        try:
            self._cur.execute(scripts_database.UPDATE_GET_NEXT_UPLOAD, params)
            row = self._cur.fetchone()
        finally:
            self._cur.close()

        if row is not None:
            fuuid, taille = row
            return {'fuuid': fuuid, 'taille': taille}

        return None

    def touch_download(self, fuuid: str, erreur: Optional[int] = None):
        params = {'fuuid': fuuid, 'date_activite': datetime.datetime.now(tz=pytz.UTC), 'erreur': erreur}
        self._cur.execute(scripts_database.UPDATE_TOUCH_DOWNLOAD, params)

    def get_batch_backups_primaire(self) -> list[dict]:
        self._cur.execute(scripts_database.UPDATE_FETCH_BACKUP_PRIMAIRE)
        rows = self._cur.fetchall()
        tasks = list()
        if rows is not None and len(rows) > 0:
            for row in rows:
                uuid_backup, domaine, nom_fichier, taille = row
                tasks.append({'uuid_backup': uuid_backup, 'domaine': domaine, 'nom_fichier': nom_fichier, 'taille': taille})
        return tasks

    def supprimer_job_download(self, fuuid: str):
        params = {'fuuid': fuuid}
        self._cur.execute(scripts_database.DELETE_DOWNLOAD, params)

    def supprimer_job_upload(self, fuuid: str):
        params = {'fuuid': fuuid}
        self._cur.execute(scripts_database.DELETE_UPLOAD, params)

    def touch_upload(self, fuuid: str, erreur: Optional[int] = None):
        params = {'fuuid': fuuid, 'date_activite': datetime.datetime.now(tz=pytz.UTC), 'erreur': erreur}
        self._cur.execute(scripts_database.UPDATE_TOUCH_UPLOAD, params)

    def ajouter_fichier_manquant(self, fuuid) -> bool:
        """ Ajoute un fichier qui devrait etre manquant (e.g. sur evenement consignationPrimaire)"""
        ajoute = False
        date_now = datetime.datetime.now(tz=pytz.UTC)
        params = {
            'fuuid': fuuid,
            'etat_fichier': Constantes.DATABASE_ETAT_MANQUANT,
            'taille': None,
            'bucket': Constantes.BUCKET_PRINCIPAL,
            'date_presence': date_now,
            'date_verification': date_now,
            'date_reclamation': date_now,
        }
        try:
            self._cur.execute(scripts_database.INSERT_FICHIER, params)
            ajoute = True
        except sqlite3.IntegrityError as e:
            pass  # OK, le fichier existe deja

        return ajoute

    def ajouter_download_primaire(self, fuuid: str, taille: int):
        params = {
            'fuuid': fuuid,
            'taille': taille,
            'date_creation': datetime.datetime.now(tz=pytz.UTC),
        }
        try:
            self._cur.execute(scripts_database.INSERT_DOWNLOAD, params)
        except sqlite3.IntegrityError as e:
            pass  # OK, le fichier existe deja dans la liste de downloads

    def ajouter_upload_secondaire_conditionnel(self, fuuid: str) -> bool:
        """ Ajoute conditionnellement un upload vers le primaire """
        self._cur.execute(scripts_database.SELECT_PRIMAIRE_PAR_FUUID, {'fuuid': fuuid})
        row = self._cur.fetchone()
        if row is not None:
            _fuuid, etat_fichier, taille, bucket = row
            if etat_fichier != 'manquant':
                # Le fichier n'est pas manquant (il est deja sur le primaire). SKIP
                return False

        # Le fichier est inconnu ou manquant sur le primaire. On peut creer l'upload
        self._cur.execute(scripts_database.SELECT_FICHIER_PAR_FUUID, {'fuuid': fuuid})
        row = self._cur.fetchone()
        if row is not None:
            _fuuid, etat_fichier, taille, bucket = row
            if etat_fichier == Constantes.DATABASE_ETAT_ACTIF:
                # Creer la job d'upload
                params = {
                    'fuuid': fuuid,
                    'taille': taille,
                    'date_creation': datetime.datetime.now(tz=pytz.UTC),
                }
                try:
                    self._cur.execute(scripts_database.INSERT_UPLOAD, params)
                except sqlite3.IntegrityError as e:
                    pass  # OK, le fichier existe deja dans la liste d'uploads
                return True

        # Le fichier est inconnu localement ou inactif
        return False


class SQLiteBatchOperations(SQLiteCursor):

    def __init__(self, connection: SQLiteConnection, batch_size=250):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        super().__init__(connection)

        self.__batch: Optional[list] = None
        self.__limite_batch = batch_size
        self.__debut_job = datetime.datetime.now(tz=pytz.UTC)

        self.__write_lock = connection.locks.write
        self.__batch_lock = connection.locks.batch_job

        self.__batch_script: Optional[str] = None

    @property
    def batch_script(self) -> Optional[str]:
        return self.__batch_script

    @batch_script.setter
    def batch_script(self, batch_script: str):
        if self.__batch_script != batch_script and self.__batch_script is not None and self.__batch is not None:
            raise Exception('batch en cours')
        self.__batch_script = batch_script

    async def __aenter__(self):
        await self.__batch_lock.acquire()
        try:
            self.open()
        except Exception as e:
            self.__batch_lock.release()
            raise e

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            await self.commit_batch()
        finally:
            try:
                self.close()
            finally:
                self.__batch_lock.release()
        return False

    async def commit_batch(self):
        batch = self.__batch
        self.__batch = None
        try:
            if batch is not None:
                if self.__batch_script is None:
                    raise Exception('batch script is None')
                async with self.__write_lock:
                    resultat = await asyncio.to_thread(self._cur.executemany, self.__batch_script, batch)
            else:
                resultat = None
        finally:
            await asyncio.to_thread(self._connection.commit)

        return batch, resultat

    def close(self):
        self._cur.close()
        self._cur = None

    def ajouter_item_batch(self, item: dict):
        if self.__batch is None:
            self.__batch = list()
        self.__batch.append(item)

    def ajouter_visite(self, bucket: str, fuuid: str, taille: int) -> int:
        self.batch_script = scripts_database.INSERT_PRESENCE_FICHIERS
        row = {
            'fuuid': fuuid,
            'etat_fichier': Constantes.DATABASE_ETAT_ACTIF,
            'taille': taille,
            'bucket': bucket,
            'date_presence': datetime.datetime.now(tz=pytz.UTC),
        }
        self.ajouter_item_batch(row)
        return len(self.__batch)

    def marquer_actifs_visites(self):
        """ Sert a marquer tous les fichiers "manquants" comme actifs si visites recemment. """
        try:
            resultat = self._cur.execute(scripts_database.UPDATE_ACTIFS_VISITES, {'date_presence': self.__debut_job})
            self.__logger.info("marquer_actifs_visites Marquer manquants comme actifs si visite >= %s : %d rows" %
                               (self.__debut_job, resultat.rowcount))
            self._connection.commit()

            resultat = self._cur.execute(scripts_database.UPDATE_MANQANTS_VISITES, {'date_presence': self.__debut_job})
            self.__logger.info("marquer_actifs_visites Marquer actifs,orphelins comme manquants si visite < %s : %d rows" %
                               (self.__debut_job, resultat.rowcount))
        finally:
            self._connection.commit()

    async def ajouter_fichier_primaire(self, fichier: dict) -> bool:
        """

        :param fichier:
        :return: True si commit complete
        """
        self.batch_script = scripts_database.INSERT_FICHIER_PRIMAIRE
        self.ajouter_item_batch(fichier)

        if len(self.__batch) >= self.__limite_batch:
            await self.commit_batch()
            return True

        return False

    async def ajouter_backup_primaire(self, fichier: dict):
        self.batch_script = scripts_database.INSERT_BACKUP_PRIMAIRE
        self.ajouter_item_batch(fichier)

        if len(self.__batch) >= self.__limite_batch:
            await self.commit_batch()

    async def marquer_secondaires_reclames(self):
        # Inserer secondaires manquants
        async with self.__write_lock:
            params = {'date_reclamation': datetime.datetime.now(tz=pytz.UTC)}
            await asyncio.to_thread(self._cur.execute, scripts_database.INSERT_SECONDAIRES_MANQUANTS, params)
            # self.__con.commit()

            # Convertir les secondaires orphelins en actifs
            await asyncio.to_thread(self._cur.execute, scripts_database.UPDATE_SECONDAIRES_ORPHELINS_VERS_ACTIF, params)
            # self.__con.commit()

            # Marquer les secondaires deja presents comme reclames (peu importe l'etat)
            params = {'date_reclamation': datetime.datetime.now(tz=pytz.UTC)}
            await asyncio.to_thread(self._cur.execute, scripts_database.UPDATE_SECONDAIRES_RECLAMES, params)
            # self.__con.commit()

            params = {'date_reclamation': self.__debut_job}
            await asyncio.to_thread(self._cur.execute, scripts_database.UPDATE_SECONDAIRES_NON_RECLAMES_VERS_ORPHELINS, params)

            await asyncio.to_thread(self._connection.commit)

    async def generer_downloads(self):
        params = {'date_creation': datetime.datetime.now(tz=pytz.UTC)}
        async with self.__write_lock:
            await asyncio.to_thread(self._cur.execute, scripts_database.INSERT_DOWNLOADS, params)
            await asyncio.to_thread(self._connection.commit)

    async def generer_uploads(self):
        params = {'date_creation': datetime.datetime.now(tz=pytz.UTC)}
        async with self.__write_lock:
            await asyncio.to_thread(self._cur.execute, scripts_database.INSERT_UPLOADS, params)
            await asyncio.to_thread(self._connection.commit)

    async def entretien_transferts(self):
        """ Marque downloads ou uploads expires, permet nouvel essai. """
        now = datetime.datetime.now(tz=pytz.UTC)

        async with self.__write_lock:
            await asyncio.to_thread(self._cur.execute, scripts_database.DELETE_DOWNLOADS_ESSAIS_EXCESSIFS)

            expiration_downloads = now - datetime.timedelta(minutes=30)
            params = {'date_activite': expiration_downloads}
            await asyncio.to_thread(self._cur.execute, scripts_database.UPDATE_RESET_DOWNLOAD_EXPIRE, params)

            expiration_uploads = now - datetime.timedelta(minutes=30)
            params = {'date_activite': expiration_uploads}
            await asyncio.to_thread(self._cur.execute, scripts_database.UPDATE_RESET_UPLOADS_EXPIRE, params)

            await asyncio.to_thread(self._connection.commit)

    async def marquer_verification(self, fuuid: str, etat_fichier: str):
        async with self.__write_lock:
            if etat_fichier == Constantes.DATABASE_ETAT_ACTIF:
                # Mettre a jour la date de verification
                params = {
                    'fuuid': fuuid,
                    'date_verification': datetime.datetime.now(tz=pytz.UTC)
                }
                await asyncio.to_thread(self._cur.execute, scripts_database.UPDATE_DATE_VERIFICATION, params)
            else:
                # Erreur - mettre a jour l'etat seulement
                params = {
                    'fuuid': fuuid,
                    'etat_fichier': etat_fichier
                }
                await asyncio.to_thread(self._cur.execute, scripts_database.UPDATE_DATE_ETATFICHIER, params)

            # Pas de batch, on commit chaque fichier (verification fuuid est intensive, grand delai entre operations)
            await asyncio.to_thread(self._connection.commit)

    async def supprimer(self, fuuid: str):
        self.batch_script = scripts_database.DELETE_SUPPRIMER_FUUIDS
        self.ajouter_item_batch({'fuuid': fuuid})

        if len(self.__batch) >= self.__limite_batch:
            await self.commit_batch()
