import asyncio
import datetime
import json
import logging
import pathlib
import sqlite3
from typing import Optional

import pytz

from millegrilles_fichiers import Constantes, DatabaseScripts as scripts_database

# sqlite3.threadsafety = 3


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

    def __init__(self, path_database: pathlib.Path, locks: Optional[SQLiteLocks] = None,
                 check_same_thread=True, timeout=5.0, reuse=False):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__path_database = path_database
        self.__path_data = path_database.parent
        self.__sqlite_locks = locks
        self.__check_same_thread = check_same_thread
        self.__timeout = timeout
        self.__reuse = reuse

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
        if self.__reuse is False:
            self.__con.close()
            self.__con = None

    def init_database(self):
        cur = self.__con.cursor()
        try:
            self.__con.executescript(scripts_database.CONST_CREATE_FICHIERS)
        finally:
            cur.close()
            self.__con.commit()

    def __enter__(self):
        if self.__con is None:
            self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.close()
        except sqlite3.OperationalError as e:
            self.__logger.exception("Operational error sur close/commit : %s" % str(e))

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
        await asyncio.to_thread(self.close)
        return False

    def reset_intermediaires(self):
        path_intermediaires = self.get_path_relamations_intermediaires()
        path_intermediaires.unlink(missing_ok=True)

    def get_path_database(self):
        return self._connection.path_database

    def get_path_relamations_intermediaires(self):
        return pathlib.Path(self._connection.path_data, Constantes.FICHIER_RECLAMATIONS_INTERMEDIAIRES)

    async def commit(self):
        await asyncio.to_thread(self._connection.commit)


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

    def charger_verifier_fuuids(self, limite_taille: Constantes.CONST_LIMITE_TAILLE_VERIFICATION) -> list[dict]:
        # Generer une batch de fuuids a verifier
        limite_nombre = 1000

        # La reverification permet de controler la frequence de verification d'un fichier (e.g. aux trois mois)
        expiration = datetime.datetime.now(tz=pytz.UTC) - datetime.timedelta(seconds=Constantes.CONST_INTERVALLE_REVERIFICATION)
        params = {
            'expiration_verification': expiration,
            'limit': limite_nombre
        }

        self._cur.execute(scripts_database.SELECT_BATCH_VERIFIER, params)

        taille_totale = 0
        fuuids = list()
        while True:
            row = self._cur.fetchone()
            if row is None or taille_totale > limite_taille:
                break
            fuuid, taille, bucket_visite = row
            if isinstance(taille, int):
                taille_totale += taille
                if len(fuuids) > 0 and taille_totale > limite_taille:
                    break  # On a atteint la limite en bytes
                fuuids.append({'fuuid': fuuid, 'taille': taille, 'bucket': bucket_visite})
            else:
                self.__logger.warning('fuuid %s avec taille NULL, skip' % fuuid)

        return fuuids

    def get_stats_fichiers(self) -> dict:
        self._cur.execute(scripts_database.SELECT_STATS_FICHIERS)

        resultats_dict = dict()
        nombre_orphelins = 0
        taille_orphelins = 0
        nombre_manquants = 0

        while True:
            row = self._cur.fetchone()
            if row is None:
                break

            etat_fichier, bucket, nombre, taille = row
            if etat_fichier == Constantes.DATABASE_ETAT_ACTIF:
                resultats_dict[bucket] = {
                    'nombre': nombre,
                    'taille': taille
                }
            elif etat_fichier == Constantes.DATABASE_ETAT_ORPHELIN:
                nombre_orphelins += nombre
                taille_orphelins += taille
            elif etat_fichier == Constantes.DATABASE_ETAT_MANQUANT:
                nombre_manquants += nombre

        resultats_dict[Constantes.DATABASE_ETAT_ORPHELIN] = {
            'nombre': nombre_orphelins,
            'taille': taille_orphelins
        }

        resultats_dict[Constantes.DATABASE_ETAT_MANQUANT] = {
            'nombre': nombre_manquants,
        }

        return resultats_dict

    def get_info_fichier(self, fuuid: str):
        self._cur.execute(scripts_database.SELECT_INFO_FICHIER, {'fuuid': fuuid})
        row = self._cur.fetchone()
        if row is not None:
            _fuuid, etat_fichier, taille, bucket, date_presence, date_verification, date_reclamation = row

            return {
                'fuuid': fuuid,
                'taille': taille,
                'etat_fichier': etat_fichier,
                'date_presence': date_presence,
                'date_verification': date_verification,
                'date_reclamation': date_reclamation
            }
        else:
            return None

    def get_backup_batch(self, params: dict, limite_taille=500_000_000) -> list:
        self._cur.execute(scripts_database.SELECT_BACKUP_STORE_FICHIERS, params)

        taille_totale = 0
        fuuids = list()
        while True:
            row = self._cur.fetchone()
            if row is None:
                break

            fuuid, taille, bucket = row
            if isinstance(taille, int):
                if len(fuuids) > 0 and taille_totale + taille > limite_taille:
                    continue  # On a atteint la limite en bytes, tenter de trouver des fichiers plus petits
                taille_totale += taille
                fuuids.append({'fuuid': fuuid, 'taille': taille, 'bucket': bucket})
            else:
                self.__logger.warning('fuuid %s avec taille NULL, skip' % fuuid)

        return fuuids


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

    def touch_backup_fichier(self, fuuid: str, taille: int):
        # Note : la taille est utilisee pour s'assurer un match sur le contenu transfere
        params = {'fuuid': fuuid, 'taille': taille, 'date_backup': datetime.datetime.now(tz=pytz.UTC)}
        self._cur.execute(scripts_database.UPDATE_TOUCH_BACKUP_FICHIER, params)

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

    def activer_si_orphelin(self, fuuids: list[str], date_reclamation: datetime.datetime) -> list[str]:
        dict_fuuids = dict()
        idx = 0
        for fuuid in fuuids:
            dict_fuuids['f%d' % idx] = fuuid
            idx += 1

        params = {
            'date_reclamation': date_reclamation
        }
        params.update(dict_fuuids)

        requete = scripts_database.UPDATE_ACTIVER_SI_ORPHELIN.replace('$fuuids', ','.join([':%s' % f for f in dict_fuuids.keys()]))

        self._cur.execute(requete, params)

        return dict_fuuids.keys()

    def get_info_fichiers_actif(self, fuuid_keys: list) -> list:
        liste_fichiers_actifs = list()
        requete = scripts_database.SELECT_INFO_FICHIERS_ACTIFS.replace('$fuuids', ','.join([':%s' % f for f in fuuid_keys]))
        self._cur.execute(requete, fuuid_keys)

        while True:
            row = self._cur.fetchone()
            if row is None:
                break
            fuuid = row[0]
            liste_fichiers_actifs.append(fuuid)

        return liste_fichiers_actifs


class SQLiteBatchOperations(SQLiteCursor):

    def __init__(self, connection: SQLiteConnection):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        super().__init__(connection)

        self.__debut_job = datetime.datetime.now(tz=pytz.UTC)

        self.__write_lock = connection.locks.write
        self.__batch_lock = connection.locks.batch_job

    def attach(self, db_path: pathlib.Path, alias='attached'):
        sql = "ATTACH DATABASE :db AS %s" % alias
        params = {'db': str(db_path)}
        return self._cur.execute(sql, params)

    def detach(self, alias='attached'):
        sql = "DETACH DATABASE %s" % alias
        return self._cur.execute(sql)

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
            self.close()
        finally:
            self.__batch_lock.release()
        return False

    # async def commit_batch(self):
    #     batch = self.__batch
    #     self.__batch = None
    #     self.open()
    #     try:
    #         if batch is not None:
    #             if self.__batch_script is None:
    #                 raise Exception('batch script is None')
    #             async with self.__write_lock:
    #                 resultat = await asyncio.to_thread(self._cur.executemany, self.__batch_script, batch)
    #         else:
    #             resultat = None
    #     finally:
    #         try:
    #             await asyncio.to_thread(self._connection.commit)
    #         finally:
    #             self.close()
    #
    #     await asyncio.sleep(0.5)  # Intervalle, laisser le systeme effectuer autres operations
    #
    #     return batch, resultat

    def close(self):
        self._cur.close()
        self._cur = None

    # def ajouter_item_batch(self, item: dict):
    #     if self.__batch is None:
    #         self.__batch = list()
    #     self.__batch.append(item)

    # def ajouter_visite(self, bucket: str, fuuid: str, taille: int) -> int:
    #     self.batch_script = scripts_database.INSERT_PRESENCE_FICHIERS
    #     row = {
    #         'fuuid': fuuid,
    #         'etat_fichier': Constantes.DATABASE_ETAT_ACTIF,
    #         'taille': taille,
    #         'bucket': bucket,
    #         'date_presence': datetime.datetime.now(tz=pytz.UTC),
    #         'date_verification': datetime.datetime.fromtimestamp(0, tz=pytz.UTC),
    #     }
    #     self.ajouter_item_batch(row)
    #     return len(self.__batch)

    def marquer_actifs_visites(self):
        """ Sert a marquer tous les fichiers "manquants" comme actifs si visites recemment. """
        self.open()
        try:
            resultat = self._cur.execute(scripts_database.UPDATE_ACTIFS_VISITES, {'date_presence': self.__debut_job})
            self.__logger.info("marquer_actifs_visites Marquer manquants comme actifs si visite >= %s : %d rows" %
                               (self.__debut_job, resultat.rowcount))
            self._connection.commit()

            resultat = self._cur.execute(scripts_database.UPDATE_MANQANTS_VISITES, {'date_presence': self.__debut_job})
            self.__logger.info("marquer_actifs_visites Marquer actifs,orphelins comme manquants si visite < %s : %d rows" %
                               (self.__debut_job, resultat.rowcount))
        finally:
            try:
                self._connection.commit()
            finally:
                self.close()

    async def marquer_secondaires_reclames(self):
        # Inserer secondaires manquants
        self.open()
        async with self.__write_lock:
            try:
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
            finally:
                self.close()

    async def generer_downloads(self):
        params = {'date_creation': datetime.datetime.now(tz=pytz.UTC)}
        self.open()
        async with self.__write_lock:
            try:
                await asyncio.to_thread(self._cur.execute, scripts_database.INSERT_DOWNLOADS, params)
                await asyncio.to_thread(self._connection.commit)
            finally:
                self.close()

    async def generer_uploads(self):
        params = {'date_creation': datetime.datetime.now(tz=pytz.UTC)}
        self.open()
        async with self.__write_lock:
            try:
                await asyncio.to_thread(self._cur.execute, scripts_database.INSERT_UPLOADS, params)
                await asyncio.to_thread(self._connection.commit)
            finally:
                self.close()

    async def entretien_transferts(self):
        """ Marque downloads ou uploads expires, permet nouvel essai. """
        now = datetime.datetime.now(tz=pytz.UTC)

        self.open()
        async with self.__write_lock:
            try:
                await asyncio.to_thread(self._cur.execute, scripts_database.DELETE_DOWNLOADS_ESSAIS_EXCESSIFS)

                expiration_downloads = now - datetime.timedelta(minutes=30)
                params = {'date_activite': expiration_downloads}
                await asyncio.to_thread(self._cur.execute, scripts_database.UPDATE_RESET_DOWNLOAD_EXPIRE, params)

                expiration_uploads = now - datetime.timedelta(minutes=30)
                params = {'date_activite': expiration_uploads}
                await asyncio.to_thread(self._cur.execute, scripts_database.UPDATE_RESET_UPLOADS_EXPIRE, params)

                await asyncio.to_thread(self._connection.commit)
            finally:
                self.close()

    async def marquer_verification(self, fuuid: str, etat_fichier: str):
        self.open()
        async with self.__write_lock:
            try:
                if etat_fichier == Constantes.DATABASE_ETAT_ACTIF:
                    # Mettre a jour la date de verification
                    params = {
                        'fuuid': fuuid,
                        'date_verification': datetime.datetime.now(tz=pytz.UTC)
                    }
                    await asyncio.to_thread(self._cur.execute, scripts_database.UPDATE_DATE_VERIFICATION, params)
                else:
                    # Erreur - mettre a jour l'etat et resetter la presence
                    params = {
                        'fuuid': fuuid,
                        'etat_fichier': etat_fichier,
                        'date_verification': datetime.datetime.now(tz=pytz.UTC),
                        'date_presence': datetime.datetime.fromtimestamp(0, tz=pytz.UTC),
                    }
                    await asyncio.to_thread(self._cur.execute, scripts_database.UPDATE_DATE_ETATFICHIER_PRESENCE, params)

                # Pas de batch, on commit chaque fichier (verification fuuid est intensive, grand delai entre operations)
                await asyncio.to_thread(self._connection.commit)
            finally:
                self.close()

    async def supprimer(self, fuuid: str):
        self.batch_script = scripts_database.DELETE_SUPPRIMER_FUUIDS
        self.ajouter_item_batch({'fuuid': fuuid})

        if len(self.__batch) >= self.__limite_batch:
            await self.commit_batch()

    def marquer_orphelins(self, debut_reclamation: datetime.datetime):
        self.open()
        try:
            resultat = self._cur.execute(scripts_database.UPDATE_MARQUER_ORPHELINS, {'date_reclamation': debut_reclamation})
            return resultat
        finally:
            self.close()

    def marquer_actifs(self, debut_reclamation: datetime.datetime):
        self.open()
        try:
            resultat = self._cur.execute(scripts_database.UPDATE_MARQUER_ACTIF, {'date_reclamation': debut_reclamation})
            return resultat
        finally:
            self.close()


class SQLiteDetachedOperations(SQLiteCursor):

    def __init__(self, connection_destination: SQLiteConnection, database_work, delete_db=False):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

        self._connection_destination = connection_destination
        self.__path_database_detached = pathlib.Path(database_work)
        self.__delete_db = delete_db

        self.__debut_job = datetime.datetime.now(tz=pytz.UTC)

        self.__conn_detached = SQLiteConnection(
            self.__path_database_detached, locks=None, check_same_thread=False, timeout=1.0, reuse=False)

        super().__init__(self.__conn_detached)

    def get_path_database_destination(self):
        return self._connection_destination.path_database

    async def attach_destination(self):
        db_path = self.get_path_database_destination()
        sql = "ATTACH DATABASE :db AS destination"
        params = {'db': str(db_path)}
        return await asyncio.to_thread(self._cur.execute, sql, params)

    async def detach_destination(self):
        sql = "DETACH DATABASE destination"
        return await asyncio.to_thread(self._cur.execute, sql)

    async def _create(self):
        """ Initialise la base de donnees (create tables, indexes, etc.) """
        raise NotImplementedError('must implement')

    async def _transfer_data(self):
        """ Effectue les operations sur la destination """
        raise NotImplementedError('must implement')

    async def __aenter__(self):
        if self.__delete_db:
            self.__path_database_detached.unlink(missing_ok=True)

        # Initialiser la base de donnees
        self.__conn_detached.open()
        try:
            self.open()

            await self._create()
        except:
            self.__logger.exception("__aenter__ Erreur oouverture db %s" % self.__path_database_detached)
            self.__conn_detached.close()

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            await self._transfer_data()
        finally:
            try:
                self.close()
            finally:
                self.__conn_detached.close()
        return False

    def close(self):
        self._cur.close()
        self._cur = None

    # def ajouter_visite(self, bucket: str, fuuid: str, taille: int) -> int:
    #     self.batch_script = scripts_database.INSERT_PRESENCE_FICHIERS
    #     row = {
    #         'fuuid': fuuid,
    #         'etat_fichier': Constantes.DATABASE_ETAT_ACTIF,
    #         'taille': taille,
    #         'bucket': bucket,
    #         'date_presence': datetime.datetime.now(tz=pytz.UTC),
    #         'date_verification': datetime.datetime.fromtimestamp(0, tz=pytz.UTC),
    #     }
    #     self.ajouter_item_batch(row)
    #     return len(self.__batch)
    #
    # async def ajouter_fichier_primaire(self, fichier: dict) -> bool:
    #     """
    #
    #     :param fichier:
    #     :return: True si commit complete
    #     """
    #     self.batch_script = scripts_database.INSERT_FICHIER_PRIMAIRE
    #     self.ajouter_item_batch(fichier)
    #
    #     if len(self.__batch) >= self.__limite_batch:
    #         await self.commit_batch()
    #         return True
    #
    #     return False
    #
    # async def ajouter_backup_primaire(self, fichier: dict):
    #     self.batch_script = scripts_database.INSERT_BACKUP_PRIMAIRE
    #     self.ajouter_item_batch(fichier)
    #
    #     if len(self.__batch) >= self.__limite_batch:
    #         await self.commit_batch()
    #
    # async def ajouter_reclamer_fichier(self, fuuid: str, bucket: str):
    #     self.batch_script = scripts_database.INSERT_RECLAMER_FICHIER
    #
    #     row = {
    #         'fuuid': fuuid,
    #         'etat_fichier': Constantes.DATABASE_ETAT_MANQUANT,
    #         'bucket': bucket,
    #         'date_reclamation': datetime.datetime.now(tz=pytz.UTC),
    #         'date_verification': datetime.datetime.fromtimestamp(0, tz=pytz.UTC)
    #     }
    #     self.ajouter_item_batch(row)
    #
    #     if len(self.__batch) >= self.__limite_batch:
    #         return await self.commit_batch()
    #
    #     return False
    #
    # async def ajouter_backup_consignation(self, fuuid, taille):
    #     self.batch_script = scripts_database.UPDATE_TOUCH_BACKUP_FICHIER
    #
    #     params = {'fuuid': fuuid, 'taille': taille, 'date_backup': datetime.datetime.now(tz=pytz.UTC)}
    #     self.ajouter_item_batch(params)
    #
    #     if len(self.__batch) >= self.__limite_batch:
    #         return await self.commit_batch()
    #
    #     return False


class SQLiteDetachedSyncCreate(SQLiteDetachedOperations):
    """ Initialisation (reset) de la base de donnees de sync pour commencer une nouvelle synchronisation """

    def __init__(self, connection_destination: SQLiteConnection, database_work):
        super().__init__(connection_destination, database_work, True)

    async def _create(self):
        await asyncio.to_thread(self._cur.executescript, scripts_database.CONST_CREATE_SYNC)

    async def _transfer_data(self):
        pass  # Rien a faire


class SQLiteDetachedReclamationAppend(SQLiteDetachedOperations):
    """ Appende de donnes de reclamations """

    def __init__(self, connection_destination: SQLiteConnection, database_work):
        super().__init__(connection_destination, database_work, False)

    async def _create(self):
        # self._cur.execute(scripts_database.CONST_CREATE_SYNC)
        pass  # Rien a faire

    async def _transfer_data(self):
        pass  # Rien a faire, reclamation en cours

    async def reclamer_fuuids(self, fuuids: list, bucket: str):
        batch = list()
        date_reclamation = datetime.datetime.now(tz=pytz.UTC)
        for fuuid in fuuids:
            batch.append({'fuuid': fuuid, 'bucket': bucket, 'date_reclamation': date_reclamation})

        return await asyncio.to_thread(self._cur.executemany, scripts_database.INSERT_RECLAMER_FICHIER, batch)


class SQLiteDetachedVisiteAppend(SQLiteDetachedOperations):
    """ Appende de donnes de reclamations """

    def __init__(self, connection_destination: SQLiteConnection, database_work):
        super().__init__(connection_destination, database_work, False)
        self.__batch = list()
        self.__batch_size = 10_000

    async def _create(self):
        pass  # Rien a faire

    async def _transfer_data(self):
        pass  # Rien a faire, reclamation en cours

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if len(self.__batch) > 0:
            raise Exception('traitement batch incomplet - utiliser commit_batch')
        return await super().__aexit__(exc_type, exc_val, exc_tb)

    async def ajouter_visite(self, fuuid: str, bucket: str, taille: int) -> Optional[list]:
        param = {'fuuid': fuuid, 'bucket': bucket, 'taille': taille, 'date_presence': datetime.datetime.now(tz=pytz.UTC)}
        self.__batch.append(param)

        if len(self.__batch) >= self.__batch_size:
            batch, resultat = await self.commit_batch()
            return batch

    async def commit_batch(self):
        batch = self.__batch
        resultat = await asyncio.to_thread(self._cur.executemany, scripts_database.INSERT_PRESENCE_FICHIERS, batch)
        self.__batch = list()  # Nouvelle liste
        return batch, resultat


class SQLiteDetachedSyncTransfer(SQLiteDetachedOperations):

    def __init__(self, connection_destination: SQLiteConnection, database_work):
        super().__init__(connection_destination, database_work, False)

    async def _create(self):
        pass  # Rien a faire

    async def _transfer_data(self):
        await self.attach_destination()

        await self.__transfert_actifs()
        await asyncio.to_thread(self._connection.commit)
        await asyncio.sleep(1)

        await self.__transfert_manquants()
        await asyncio.to_thread(self._connection.commit)
        await asyncio.sleep(1)

        await self.__transfert_orphelins()
        await self.__transfer_supprimes()

    async def __transfert_actifs(self):
        """ Process fichiers actifs - upsert dans table fichiers """
        await asyncio.to_thread(self._cur.execute, scripts_database.TRANSFERT_INSERT_PRESENCE_FICHIERS)

    async def __transfert_manquants(self):
        """ Process fichiers manquants (reclames sans presence) - upsert dans table fichiers """
        await asyncio.to_thread(self._cur.execute, scripts_database.TRANSFERT_INSERT_MANQUANTS_FICHIERS)

    async def __transfert_orphelins(self):
        """ Process fichiers orphelins (presence sans reclamation) - upsert dans table fichiers """
        await asyncio.to_thread(self._cur.execute, scripts_database.TRANSFERT_INSERT_ORPHELINS_FICHIERS)

    async def __transfer_supprimes(self):
        """ Process fichiers a supprimer (absents du sync) """
        pass


class SQLiteDetachedBackup(SQLiteDetachedOperations):
    """ Append des donnees de backup """

    def __init__(self, connection_destination: SQLiteConnection, database_work, delete_db=True):
        super().__init__(connection_destination, database_work, delete_db)
        self.__batch = list()
        self.__batch_size = 10_000

    async def _create(self):
        await asyncio.to_thread(self._cur.executescript, scripts_database.CONST_CREATE_BACKUP)

    async def _transfer_data(self):
        await self.attach_destination()
        await asyncio.to_thread(self._cur.execute, scripts_database.TRANSFERT_UPDATE_BACKUPS)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if len(self.__batch) > 0:
            raise Exception('traitement batch incomplet - utiliser commit_batch')
        return await super().__aexit__(exc_type, exc_val, exc_tb)

    async def ajouter_backup_consignation(self, fuuid: str, taille: int) -> Optional[list]:
        param = {'fuuid': fuuid, 'taille': taille, 'date_backup': datetime.datetime.now(tz=pytz.UTC)}
        self.__batch.append(param)

        if len(self.__batch) >= self.__batch_size:
            batch, resultat = await self.commit_batch()
            return batch

    async def commit_batch(self):
        batch = self.__batch
        if len(batch) > 0:
            resultat = await asyncio.to_thread(self._cur.executemany, scripts_database.INSERT_BACKUP_FICHIER, batch)
        else:
            resultat = list()
        self.__batch = list()  # Nouvelle liste
        return batch, resultat
