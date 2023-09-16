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
import time

from aiohttp import web, ClientSession
from typing import Optional, Type, Union

import pytz

from millegrilles_messages.messages import Constantes as ConstantesMillegrilles
from millegrilles_messages.messages.Hachage import VerificateurHachage, ErreurHachage

import millegrilles_fichiers.DatabaseScripts as scripts_database
from millegrilles_fichiers import Constantes
from millegrilles_fichiers.EtatFichiers import EtatFichiers
from millegrilles_fichiers.UploadFichiersPrimaire import EtatUpload, feed_filepart2


class EntretienDatabase:

    def __init__(self, etat: EtatFichiers, check_same_thread=True, timeout=5.0):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self._etat = etat
        self.__check_same_thread = check_same_thread
        self.__timeout = timeout

        path_database = pathlib.Path(
            self._etat.configuration.dir_consignation, Constantes.DIR_DATA, Constantes.FICHIER_DATABASE)
        self.__path_database = path_database

        self.__con: Optional[sqlite3.Connection] = None  # sqlite3.connect(self.__path_database, check_same_thread=check_same_thread)
        self.__cur: Optional[sqlite3.Cursor] = None  # self.__con.cursor()

        self.__batch_visites: Optional[dict] = None

        self.__limite_batch = 250

        self.__debut_entretien = datetime.datetime.now(tz=pytz.UTC)

    def __enter__(self):
        self.__con = sqlite3.connect(
            self.__path_database,
            timeout=self.__timeout, check_same_thread=self.__check_same_thread
        )
        self.__cur = self.__con.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__con.commit()
        self.__cur.close()
        self.__con.close()
        return False

    def ajouter_visite(self, bucket: str, fuuid: str, taille: int) -> int:
        if self.__batch_visites is None:
            self.__batch_visites = list()
        row = {
            'fuuid': fuuid,
            'etat_fichier': Constantes.DATABASE_ETAT_ACTIF,
            'taille': taille,
            'bucket': bucket,
            'date_presence': datetime.datetime.now(tz=pytz.UTC),
        }
        self.__batch_visites.append(row)

        return len(self.__batch_visites)

        # if len(self.__batch_visites) >= self.__limite_batch:
        #     self.commit_visites()
        #     if batch_sleep is not None:
        #         time.sleep(batch_sleep)

    def commit_visites(self):
        batch = self.__batch_visites
        self.__batch_visites = None
        cur = self.__con.cursor()
        try:
            if batch is not None:
                resultat = cur.executemany(scripts_database.CONST_PRESENCE_FICHIERS, batch)
            else:
                resultat = None
        finally:
            cur.close()
            self.__con.commit()

        return batch, resultat

    def marquer_actifs_visites(self):
        """ Sert a marquer tous les fichiers "manquants" comme actifs si visites recemment. """
        cur = self.__con.cursor()
        try:
            resultat = cur.execute(scripts_database.UPDATE_ACTIFS_VISITES, {'date_presence': self.__debut_entretien})
            self.__logger.info("marquer_actifs_visites Marquer manquants comme actifs si visite >= %s : %d rows" %
                               (self.__debut_entretien, resultat.rowcount))
            self.__con.commit()

            resultat = cur.execute(scripts_database.UPDATE_MANQANTS_VISITES, {'date_presence': self.__debut_entretien})
            self.__logger.info("marquer_actifs_visites Marquer actifs,orphelins comme manquants si visite < %s : %d rows" %
                               (self.__debut_entretien, resultat.rowcount))
        finally:
            cur.close()
            self.__con.commit()

    def marquer_verification(self, fuuid: str, etat_fichier: str):
        if etat_fichier == Constantes.DATABASE_ETAT_ACTIF:
            # Mettre a jour la date de verification
            params = {
                'fuuid': fuuid,
                'date_verification': datetime.datetime.now(tz=pytz.UTC)
            }
            self.__cur.execute(scripts_database.UPDATE_DATE_VERIFICATION, params)
        else:
            # Erreur - mettre a jour l'etat seulement
            params = {
                'fuuid': fuuid,
                'etat_fichier': etat_fichier
            }
            self.__cur.execute(scripts_database.UPDATE_DATE_ETATFICHIER, params)
        self.__con.commit()

    def identifier_orphelins(self, expiration: datetime.datetime) -> list:
        params = {
            'date_reclamation': expiration,
            'limit': 1000,
        }
        cur = self.__con.cursor()
        orphelins = list()
        try:
            cur.execute(scripts_database.REQUETE_BATCH_ORPHELINS, params)
            while True:
                row = cur.fetchone()
                if row is None:
                    break
                fuuid, taille, bucket_visite = row
                orphelins.append({'fuuid': fuuid, 'taille': taille, 'bucket': bucket_visite})
        finally:
            cur.close()

        return orphelins

    def supprimer(self, fuuid: str):
        params = {'fuuid': fuuid}
        cur = self.__con.cursor()
        try:
            cur.execute(scripts_database.DELETE_SUPPRIMER_FUUIDS, params)
        finally:
            self.__con.commit()
            cur.close()

    def generer_relamations_primaires(self, fp):
        """ Genere le contenu du fichier de transfert d'etat fichiers.jsonl.gz """
        cur = self.__con.cursor()
        try:
            cur.execute(scripts_database.REQUETE_FICHIERS_TRANSFERT)
            while True:
                row = cur.fetchone()
                if row is None:
                    break
                fuuid, etat_fichier, taille, bucket_visite = row
                contenu_ligne = {'fuuid': fuuid, 'etat_fichier': etat_fichier, 'taille': taille, 'bucket': bucket_visite}
                json.dump(contenu_ligne, fp)
                fp.write('\n')
        finally:
            cur.close()

        self.reset_intermediaires()

    def get_path_database(self):
        dir_data = pathlib.Path(self._etat.configuration.dir_consignation, Constantes.DIR_DATA)
        return pathlib.Path(dir_data, Constantes.FICHIER_DATABASE)

    def get_path_reclamations(self):
        dir_data = pathlib.Path(self._etat.configuration.dir_consignation, Constantes.DIR_DATA)
        return pathlib.Path(dir_data, Constantes.FICHIER_RECLAMATIONS_PRIMAIRES)

    def get_path_relamations_intermediaires(self):
        dir_data = pathlib.Path(self._etat.configuration.dir_consignation, Constantes.DIR_DATA)
        return pathlib.Path(dir_data, Constantes.FICHIER_RECLAMATIONS_INTERMEDIAIRES)

    def reset_intermediaires(self):
        path_intermediaires = self.get_path_relamations_intermediaires()
        path_intermediaires.unlink(missing_ok=True)

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
            self.__cur.execute(scripts_database.CONST_INSERT_FICHIER, data)
            nouveau = True
        except sqlite3.IntegrityError as e:
            if 'FICHIERS.fuuid' in e.args[0]:
                self.__logger.debug("ConsignationStore.consigner fuuid %s existe deja - OK" % fuuid)
                resultat_update_manquant = self.__cur.execute(scripts_database.CONST_ACTIVER_SI_MANQUANT, data)
                if resultat_update_manquant.rowcount > 0:
                    # Le fichier etait manquant, on le considere nouveau
                    nouveau = True
                self.__cur.execute(scripts_database.CONST_VERIFIER_FICHIER, data)
            else:
                raise e
        finally:
            self.__con.commit()

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
        self.__cur.execute(scripts_database.COMMANDE_TRUNCATE_FICHIERS_PRIMAIRE)
        self.__con.commit()

    def ajouter_fichier_primaire(self, fichier: dict) -> bool:
        """

        :param fichier:
        :return: True si commit complete
        """
        if self.__batch_visites is None:
            self.__batch_visites = list()
        self.__batch_visites.append(fichier)

        if len(self.__batch_visites) >= self.__limite_batch:
            self.commit_fichiers_primaire()
            return True

        return False

    def commit_fichiers_primaire(self):
        batch = self.__batch_visites
        self.__batch_visites = None
        try:
            if batch is not None:
                self.__cur.executemany(scripts_database.COMMANDE_INSERT_FICHIER_PRIMAIRE, batch)
        finally:
            self.__con.commit()

    def truncate_backup_primaire(self):
        self.__cur.execute(scripts_database.COMMANDE_TRUNCATE_BACKUPS_PRIMAIRE)
        self.__con.commit()

    def ajouter_backup_primaire(self, fichier: dict):
        if self.__batch_visites is None:
            self.__batch_visites = list()
        self.__batch_visites.append(fichier)

        if len(self.__batch_visites) >= self.__limite_batch:
            self.commit_backup_primaire()

    def commit_backup_primaire(self):
        batch = self.__batch_visites
        self.__batch_visites = None
        try:
            if batch is not None:
                self.__cur.executemany(scripts_database.INSERT_BACKUP_PRIMAIRE, batch)
        finally:
            self.__con.commit()

    def marquer_secondaires_reclames(self):
        date_debut = datetime.datetime.now(tz=pytz.UTC)

        # Inserer secondaires manquants
        params = {'date_reclamation': datetime.datetime.now(tz=pytz.UTC)}
        self.__cur.execute(scripts_database.COMMANDE_INSERT_SECONDAIRES_MANQUANTS, params)
        self.__con.commit()

        # Convertir les secondaires orphelins en actifs
        self.__cur.execute(scripts_database.COMMANDE_UPDATE_SECONDAIRES_ORPHELINS_VERS_ACTIF, params)
        self.__con.commit()

        # Marquer les secondaires deja presents comme reclames (peu importe l'etat)
        params = {'date_reclamation': datetime.datetime.now(tz=pytz.UTC)}
        self.__cur.execute(scripts_database.COMMANDE_UPDATE_SECONDAIRES_RECLAMES, params)
        self.__con.commit()

        params = {'date_reclamation': date_debut}
        self.__cur.execute(scripts_database.COMMANDE_UPDATE_SECONDAIRES_NON_RECLAMES_VERS_ORPHELINS, params)
        self.__con.commit()

    def generer_downloads(self):
        params = {'date_creation': datetime.datetime.now(tz=pytz.UTC)}
        self.__cur.execute(scripts_database.COMMAND_INSERT_DOWNLOADS, params)
        self.__con.commit()

    def generer_uploads(self):
        params = {'date_creation': datetime.datetime.now(tz=pytz.UTC)}
        self.__cur.execute(scripts_database.COMMAND_INSERT_UPLOADS, params)
        self.__con.commit()

    def get_next_download(self):
        params = {'date_activite': datetime.datetime.now(tz=pytz.UTC)}
        cur = self.__con.cursor()
        row = None
        try:
            cur.execute(scripts_database.COMMANDE_GET_NEXT_DOWNLOAD, params)
            row = cur.fetchone()
        finally:
            cur.close()
            self.__con.commit()

        if row is not None:
            fuuid, taille = row
            return {'fuuid': fuuid, 'taille': taille}

        return None

    def get_next_upload(self):
        params = {'date_activite': datetime.datetime.now(tz=pytz.UTC)}
        cur = self.__con.cursor()
        row = None
        try:
            cur.execute(scripts_database.COMMANDE_GET_NEXT_UPLOAD, params)
            row = cur.fetchone()
        finally:
            cur.close()
            self.__con.commit()

        if row is not None:
            fuuid, taille = row
            return {'fuuid': fuuid, 'taille': taille}

        return None

    def touch_download(self, fuuid: str, erreur: Optional[int] = None):
        params = {'fuuid': fuuid, 'date_activite': datetime.datetime.now(tz=pytz.UTC), 'erreur': erreur}
        cur = self.__con.cursor()
        try:
            cur.execute(scripts_database.COMMANDE_TOUCH_DOWNLOAD, params)
        finally:
            cur.close()
            self.__con.commit()

    def get_etat_downloads(self):
        cur = self.__con.cursor()
        try:
            cur.execute(scripts_database.SELECT_ETAT_DOWNLOADS)
            row = cur.fetchone()
            if row:
                nombre, taille = row
                return {'nombre': nombre, 'taille': taille}
        finally:
            cur.close()

        return None

    def get_batch_backups_primaire(self) -> list[dict]:
        self.__cur.execute(scripts_database.UPDATE_FETCH_BACKUP_PRIMAIRE)
        rows = self.__cur.fetchall()
        self.__con.commit()
        tasks = list()
        if rows is not None and len(rows) > 0:
            for row in rows:
                uuid_backup, domaine, nom_fichier, taille = row
                tasks.append({'uuid_backup': uuid_backup, 'domaine': domaine, 'nom_fichier': nom_fichier, 'taille': taille})
        return tasks

    def get_etat_uploads(self):
        self.__cur.execute(scripts_database.SELECT_ETAT_UPLOADS)
        cur = self.__con.cursor()
        try:
            row = self.__cur.fetchone()
            if row:
                nombre, taille = row
                return {'nombre': nombre, 'taille': taille}
        finally:
            cur.close()

        return None

    def entretien_transferts(self):
        """ Marque downloads ou uploads expires, permet nouvel essai. """
        now = datetime.datetime.now(tz=pytz.UTC)

        self.__cur.execute(scripts_database.DELETE_DOWNLOADS_ESSAIS_EXCESSIFS)
        self.__con.commit()

        expiration_downloads = now - datetime.timedelta(minutes=30)
        params = {'date_activite': expiration_downloads}
        self.__cur.execute(scripts_database.UPDATE_RESET_DOWNLOAD_EXPIRE, params)

        expiration_uploads = now - datetime.timedelta(minutes=30)
        params = {'date_activite': expiration_uploads}
        self.__cur.execute(scripts_database.UPDATE_RESET_UPLOADS_EXPIRE, params)

        self.__con.commit()

    def supprimer_job_download(self, fuuid: str):
        params = {'fuuid': fuuid}
        self.__cur.execute(scripts_database.COMMANDE_DELETE_DOWNLOAD, params)
        self.__con.commit()

    def supprimer_job_upload(self, fuuid: str):
        params = {'fuuid': fuuid}
        self.__cur.execute(scripts_database.COMMANDE_DELETE_UPLOAD, params)
        self.__con.commit()

    def touch_upload(self, fuuid: str, erreur: Optional[int] = None):
        params = {'fuuid': fuuid, 'date_activite': datetime.datetime.now(tz=pytz.UTC), 'erreur': erreur}
        cur = self.__con.cursor()
        try:
            cur.execute(scripts_database.COMMANDE_TOUCH_UPLOAD, params)
        finally:
            cur.close()
            self.__con.commit()

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
            self.__cur.execute(scripts_database.CONST_INSERT_FICHIER, params)
            ajoute = True
        except sqlite3.IntegrityError as e:
            pass  # OK, le fichier existe deja
        finally:
            self.__con.commit()

        return ajoute

    def ajouter_download_primaire(self, fuuid: str, taille: int):
        params = {
            'fuuid': fuuid,
            'taille': taille,
            'date_creation': datetime.datetime.now(tz=pytz.UTC),
        }
        try:
            self.__cur.execute(scripts_database.COMMAND_INSERT_DOWNLOAD, params)
        except sqlite3.IntegrityError as e:
            pass  # OK, le fichier existe deja dans la liste de downloads
        finally:
            self.__con.commit()

    def ajouter_upload_secondaire_conditionnel(self, fuuid: str) -> bool:
        """ Ajoute conditionnellement un upload vers le primaire """
        self.__cur.execute(scripts_database.SELECT_PRIMAIRE_PAR_FUUID, {'fuuid': fuuid})
        row = self.__cur.fetchone()
        if row is not None:
            _fuuid, etat_fichier, taille, bucket = row
            if etat_fichier != 'manquant':
                # Le fichier n'est pas manquant (il est deja sur le primaire). SKIP
                return False

        # Le fichier est inconnu ou manquant sur le primaire. On peut creer l'upload
        self.__cur.execute(scripts_database.SELECT_FICHIER_PAR_FUUID, {'fuuid': fuuid})
        row = self.__cur.fetchone()
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
                    self.__cur.execute(scripts_database.COMMAND_INSERT_UPLOAD, params)
                except sqlite3.IntegrityError as e:
                    pass  # OK, le fichier existe deja dans la liste d'uploads
                finally:
                    self.__con.commit()
                return True

        # Le fichier est inconnu localement ou inactif
        return False

    def get_info_backup_primaire(self, uuid_backup: str, domaine: str, nom_fichier: str) -> Optional[dict]:
        params = {'uuid_backup': uuid_backup, 'domaine': domaine, 'nom_fichier': nom_fichier}
        self.__cur.execute(scripts_database.SELECT_BACKUP_PRIMAIRE, params)
        row = self.__cur.fetchone()
        self.__con.commit()
        if row is not None:
            uuid_backup, domaine, nom_fichier, taille = row
            return {
                'uuid_backup': uuid_backup,
                'domaine': domaine,
                'nom_fichier': nom_fichier,
                'taille': taille
            }

        return None

    def close(self):
        self.__con.commit()
        self.__cur.close()
        self.__con.close()


class ConsignationStore:

    def __init__(self, etat: EtatFichiers):
        self._etat = etat
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

        # Creer repertoire data, path database
        path_database = pathlib.Path(
            self._etat.configuration.dir_consignation, Constantes.DIR_DATA, Constantes.FICHIER_DATABASE)
        self.__path_database = path_database

        self._stop_store: Optional[asyncio.Event] = None

    def initialiser(self):
        self._stop_store = asyncio.Event()

        path_backup = pathlib.Path(
            self._etat.configuration.dir_consignation, Constantes.DIR_BACKUP)
        path_backup.mkdir(parents=True, exist_ok=True)

        self.__path_database.parent.mkdir(parents=True, exist_ok=True)
        con = self.ouvrir_database()
        con.executescript(scripts_database.CONST_CREATE_FICHIERS)
        con.close()

    async def run_entretien(self):
        raise NotImplementedError('must override')

    async def stop(self):
        self._stop_store.set()

    def get_path_fuuid(self, bucket: str, fuuid: str) -> pathlib.Path:
        raise NotImplementedError('must override')

    def __consigner_db(self, path_src: pathlib.Path, fuuid: str):
        stat = path_src.stat()
        with EntretienDatabase(self._etat) as entretien_db:
            entretien_db.consigner(fuuid, stat.st_size, Constantes.BUCKET_PRINCIPAL)

    async def consigner(self, path_src: pathlib.Path, fuuid: str):
        # Tenter d'inserer le fichier comme nouveau actif dans la base de donnees
        await asyncio.to_thread(self.__consigner_db, path_src, fuuid)

        # con = sqlite3.connect(self.__path_database, check_same_thread=True)
        # stat = path_src.stat()
        #
        # date_now = datetime.datetime.now(tz=pytz.UTC)
        # data = {
        #     'fuuid': fuuid,
        #     'etat_fichier': Constantes.DATABASE_ETAT_ACTIF,
        #     'taille': stat.st_size,
        #     'bucket': Constantes.BUCKET_PRINCIPAL,
        #     'date_presence': date_now,
        #     'date_verification': date_now,
        #     'date_reclamation': date_now,
        # }
        # cur = con.cursor()
        # try:
        #     cur.execute(scripts_database.CONST_INSERT_FICHIER, data)
        # except sqlite3.IntegrityError as e:
        #     if 'FICHIERS.fuuid' in e.args[0]:
        #         self.__logger.debug("ConsignationStore.consigner fuuid %s existe deja - OK" % fuuid)
        #         cur.execute(scripts_database.CONST_ACTIVER_SI_MANQUANT, data)
        #         cur.execute(scripts_database.CONST_VERIFIER_FICHIER, data)
        #     else:
        #         raise e
        # finally:
        #     cur.close()
        #     con.commit()
        #     con.close()

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

    async def entretien(self):
        """ Declenche un entretien (visite, verification, purge, etc.) """
        raise NotImplementedError('must override')

    async def visiter_fuuids(self):
        dir_buckets = pathlib.Path(self._etat.configuration.dir_consignation, Constantes.DIR_BUCKETS)
        self.__logger.info("visiter_fuuids Debut avec path buckets %s" % dir_buckets)
        with EntretienDatabase(self._etat, check_same_thread=False) as entretien_db:
            # Parcourir tous les buckets recursivement (depth-first)
            for bucket in dir_buckets.iterdir():
                self.__logger.debug("Visiter bucket %s" % bucket.name)
                await self.visiter_bucket(bucket.name, bucket, entretien_db)

            batch, resultat = await asyncio.to_thread(entretien_db.commit_visites)
            await self.emettre_batch_visites(batch, False)

            # Marquer tous les fichiers 'manquants' qui viennent d'etre visites comme actifs (utilise date debut)
            await asyncio.to_thread(entretien_db.marquer_actifs_visites)

        self.__logger.info("visiter_fuuids Fin")

    async def visiter_bucket(self, bucket: str, path_repertoire: pathlib.Path, entretien_db: EntretienDatabase):
        """ Visiter tous les fichiers presents, s'assurer qu'ils sont dans la base de donnees. """
        raise NotImplementedError('must override')

    async def get_info_fichier_backup(self, uuid_backup: str, domaine: str, nom_fichier: str) -> dict:
        """ Retourne l'information d'un fichier de backup ou FileNotFoundError si non trouve. """
        raise NotImplementedError('must override')

    async def verifier_fuuids(self, limite=Constantes.CONST_LIMITE_TAILLE_VERIFICATION):
        await asyncio.to_thread(self.__verifier_fuuids, limite)

    def __verifier_fuuids(self, limite=Constantes.CONST_LIMITE_TAILLE_VERIFICATION):
        """ Visiter tous les fichiers presents, s'assurer qu'ils sont dans la base de donnees. """
        liste_fichiers = self.__charger_verifier_fuuids(limite)

        with EntretienDatabase(self._etat) as entretien_db:
            # Verifier chaque fichier individuellement
            for fichier in liste_fichiers:
                self.verifier_fichier(entretien_db, **fichier)

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

        with EntretienDatabase(self._etat, check_same_thread=False) as entretien_db:
            batch_orphelins = await asyncio.to_thread(entretien_db.identifier_orphelins, expiration)
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
                    self.__logger.info("supprimer_orphelins Supprimer fichier orphelin expire %s" % fichier)
                    try:
                        await self.supprimer(bucket, fuuid)
                    except OSError as e:
                        if e.errno == errno.ENOENT:
                            self.__logger.warning("Erreur suppression fichier %s/%s - non trouve" % (bucket, fuuid))
                        else:
                            raise e

                    # Supprimer de la base de donnes locale
                    await asyncio.to_thread(entretien_db.supprimer, fuuid)

        pass

    def verifier_fichier(self, entretien_db: EntretienDatabase, fuuid: str, taille: int, bucket: str):
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

        fuuids_actifs = await asyncio.to_thread(self.__reactiver_fuuids, fuuids)

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

    def __reactiver_fuuids(self, fuuids: list) -> list[str]:
        con = self.ouvrir_database()
        cur = con.cursor()

        dict_fuuids = dict()
        idx = 0
        for fuuid in fuuids:
            dict_fuuids['f%d' % idx] = fuuid
            idx += 1

        params = {
            'date_reclamation': datetime.datetime.now(tz=pytz.UTC)
        }
        params.update(dict_fuuids)

        requete = scripts_database.CONST_ACTIVER_SI_ORPHELIN.replace('$fuuids', ','.join([':%s' % f for f in dict_fuuids.keys()]))

        cur.execute(requete, params)
        con.commit()

        liste_fichiers_actifs = list()
        requete = scripts_database.CONST_INFO_FICHIERS_ACTIFS.replace('$fuuids', ','.join([':%s' % f for f in dict_fuuids.keys()]))
        cur.execute(requete, dict_fuuids)

        while True:
            row = cur.fetchone()
            if row is None:
                break
            fuuid = row[0]
            liste_fichiers_actifs.append(fuuid)

        cur.close()
        con.close()

        return liste_fichiers_actifs

    def ouvrir_database(self, ro=False) -> sqlite3.Connection:
        return sqlite3.connect(self.__path_database, check_same_thread=True)

    def get_stats(self):
        con = self.ouvrir_database()
        cur = con.cursor()
        cur.execute(scripts_database.CONST_STATS_FICHIERS)

        resultats_dict = dict()
        nombre_orphelins = 0
        taille_orphelins = 0
        nombre_manquants = 0

        while True:
            row = cur.fetchone()
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

        cur.close()
        con.close()

        resultats_dict[Constantes.DATABASE_ETAT_ORPHELIN] = {
            'nombre': nombre_orphelins,
            'taille': taille_orphelins
        }

        resultats_dict[Constantes.DATABASE_ETAT_MANQUANT] = {
            'nombre': nombre_manquants,
        }

        return resultats_dict

    def get_info_fichier(self, fuuid: str) -> Optional[dict]:
        con = self.ouvrir_database()
        cur = con.cursor()
        cur.execute(scripts_database.CONST_INFO_FICHIER, {'fuuid': fuuid})

        row = cur.fetchone()
        if row is not None:
            _fuuid, etat_fichier, taille, bucket, date_presence, date_verification, date_reclamation = row
            cur.close()
            con.close()

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
        await asyncio.to_thread(self.__reclamer_fuuids_database, fuuids, bucket)

    def __reclamer_fuuids_database(self, fuuids: list, bucket: str):
        rows = list()
        for fuuid in fuuids:
            rows.append({
                'fuuid': fuuid,
                'etat_fichier': Constantes.DATABASE_ETAT_MANQUANT,
                'bucket': bucket,
                'date_reclamation': datetime.datetime.now(tz=pytz.UTC)
            })

        con = self.ouvrir_database()
        cur = con.cursor()
        cur.executemany(scripts_database.CONST_RECLAMER_FICHIER, rows)
        con.commit()
        cur.close()
        con.close()

    async def marquer_orphelins(self, debut_reclamation: datetime.datetime, complet=False):
        await asyncio.to_thread(self.__marquer_orphelins, debut_reclamation, complet)

    def __marquer_orphelins(self, debut_reclamation: datetime.datetime, complet=False):
        con = self.ouvrir_database()
        try:
            cur = con.cursor()

            if complet:
                # Marquer les fichiers avec vieille date de reclamation comme non reclames (orphelins)
                resultat = cur.execute(scripts_database.CONST_MARQUER_ORPHELINS, {'date_reclamation': debut_reclamation})
                self.__logger.info("__marquer_orphelins Marquer actif -> orphelins : %d rows" % resultat.rowcount)
                con.commit()
            else:
                self.__logger.info("__marquer_orphelins Skip, reclamation est incomplete")

            # Marquer fichiers orphelins qui viennent d'etre reclames comme actif
            resultat = cur.execute(scripts_database.CONST_MARQUER_ACTIF, {'date_reclamation': debut_reclamation})
            self.__logger.info("__marquer_orphelins Marquer orphelins -> actif : %d rows" % resultat.rowcount)
            con.commit()

            cur.close()
        finally:
            con.close()

    def __charger_verifier_fuuids(self, limite_taille: Constantes.CONST_LIMITE_TAILLE_VERIFICATION) -> list[dict]:
        # Generer une batch de fuuids a verifier
        limite_nombre = 1000

        # La reverification permet de controler la frequence de verification d'un fichier (e.g. aux trois mois)
        expiration = datetime.datetime.now(tz=pytz.UTC) - datetime.timedelta(seconds=Constantes.CONST_INTERVALLE_REVERIFICATION)
        params = {
            'expiration_verification': expiration,
            'limit': limite_nombre
        }

        con = self.ouvrir_database()
        cur = con.cursor()
        cur.execute(scripts_database.REQUETE_BATCH_VERIFIER, params)

        taille_totale = 0
        fuuids = list()
        while True:
            row = cur.fetchone()
            if row is None:
                break
            fuuid, taille, bucket_visite = row
            taille_totale += taille
            if len(fuuids) > 0 and taille_totale > limite_taille:
                break  # On a atteint la limite en bytes
            fuuids.append({'fuuid': fuuid, 'taille': taille, 'bucket': bucket_visite})

        cur.close()
        con.close()

        return fuuids

    async def generer_reclamations_sync(self):
        dir_data = pathlib.Path(self._etat.configuration.dir_consignation)
        fichier_reclamations = pathlib.Path(dir_data,
                                            Constantes.DIR_DATA, Constantes.FICHIER_RECLAMATIONS_PRIMAIRES)

        # Preparer le fichier work
        fichier_reclamations_work = pathlib.Path('%s.work' % fichier_reclamations)
        fichier_reclamations_work.unlink(missing_ok=True)

        with EntretienDatabase(self._etat, check_same_thread=False) as entretien_db:
            try:
                with gzip.open(fichier_reclamations_work, 'wt') as fichier:
                    await asyncio.to_thread(entretien_db.generer_relamations_primaires, fichier)

                # Renommer fichier .work pour remplacer le fichier de reclamations precedent
                fichier_reclamations.unlink(missing_ok=True)
                fichier_reclamations_work.rename(fichier_reclamations)
            except:
                self.__logger.exception('Erreur generation fichier reclamations')
                fichier_reclamations_work.unlink(missing_ok=True)

    async def generer_backup_sync(self):
        """ Genere le fichier backup.jsonl.gz """
        raise NotImplementedError('must implement')

    async def upload_backups_primaire(self, session: ClientSession, entretien_db):
        raise NotImplementedError('must implement')

    async def upload_backup_primaire(self, session: ClientSession, uuid_backup: str, domaine: str, nom_fichier: str, fichier):
        url_consignation_primaire = self._etat.url_consignation_primaire
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

    # async def thread_visiter(self):
    #     stop_coro = self._stop_store.wait()
    #     await asyncio.wait([stop_coro], timeout=30)
    #     while self._stop_store.is_set() is False:
    #         self.__logger.info("thread_visiter Debut visiter fuuids")
    #         try:
    #             await asyncio.to_thread(self.visiter_fuuids)
    #         except Exception:
    #             self.__logger.exception('thread_visiter Erreur visite fuuids')
    #         await asyncio.wait([stop_coro], timeout=Constantes.CONST_INTERVALLE_VISITE_MILLEGRILLE)

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

    async def visiter_bucket(self, bucket: str, path_repertoire: pathlib.Path, entretien_db: EntretienDatabase):
        for item in path_repertoire.iterdir():
            if self._stop_store.is_set():
                raise Exception('stopping')  # Stopping

            if item.is_dir():
                # Parcourir recursivement
                await self.visiter_bucket(bucket, item, entretien_db)
            elif item.is_file():
                stat = item.stat()
                # Ajouter visite, faire commit sur batch. Attendre 5 secondes entre batch (permettre acces a la DB).
                taille_batch = entretien_db.ajouter_visite(bucket, item.name, stat.st_size)
                if taille_batch >= Constantes.CONST_BATCH_VISITES:
                    batch, resultat = await asyncio.to_thread(entretien_db.commit_visites)
                    await self.emettre_batch_visites(batch, False)
                    try:
                        await asyncio.wait_for(self._stop_store.wait(), timeout=Constantes.CONST_ATTENTE_ENTRE_BATCH_VISITES)
                    except asyncio.TimeoutError:
                        pass  # OK

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

    def verifier_fichier(self, entretien_db: EntretienDatabase, fuuid: str, taille: int, bucket: str):
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
                entretien_db.marquer_verification(fuuid, Constantes.DATABASE_ETAT_ACTIF)
            except ErreurHachage:
                self.__logger.error("verifier_fichier Fichier %s est corrompu, marquer manquant" % fuuid)
                entretien_db.marquer_verification(fuuid, Constantes.DATABASE_ETAT_MANQUANT)
                path_fichier.unlink()  # Supprimer le fichier invalide
        except FileNotFoundError:
            self.__logger.error("verifier_fichier Fichier %s est absent, marquer manquant" % fuuid)
            entretien_db.marquer_verification(fuuid, Constantes.DATABASE_ETAT_MANQUANT)

    async def generer_backup_sync(self):
        await asyncio.to_thread(self.__generer_backup_sync)

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

    async def upload_backups_primaire(self, session: ClientSession, entretien_db: EntretienDatabase):
        path_backup = pathlib.Path(self._etat.configuration.dir_consignation, Constantes.DIR_BACKUP)
        for path_uuid_backup in path_backup.iterdir():
            uuid_backup = path_uuid_backup.name
            for path_domaine in path_uuid_backup.iterdir():
                domaine = path_domaine.name
                for path_fichier in path_domaine.iterdir():
                    nom_fichier = path_fichier.name

                    info = await asyncio.to_thread(
                        entretien_db.get_info_backup_primaire, uuid_backup, domaine, nom_fichier)

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
