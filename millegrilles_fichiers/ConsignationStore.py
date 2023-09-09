import asyncio
import datetime
import errno
import logging
import pathlib
import shutil
import sqlite3
import tempfile

from aiohttp import web
from typing import Optional, Type

import pytz

from millegrilles_messages.messages import Constantes as ConstantesMillegrilles

import millegrilles_fichiers.DatabaseScripts as scripts_database
from millegrilles_fichiers import Constantes
from millegrilles_fichiers.EtatFichiers import EtatFichiers


class EntretienDatabase:

    def __init__(self, etat: EtatFichiers):
        self._etat = etat

        path_database = pathlib.Path(
            self._etat.configuration.dir_consignation, Constantes.DIR_DATA, Constantes.FICHIER_DATABASE)
        self.__path_database = path_database

        self.__con = sqlite3.connect(self.__path_database, check_same_thread=True)
        self.__cur = self.__con.cursor()

        self.__batch_visites: Optional[dict] = None

        self.__limite_batch = 100

    def ajouter_visite(self, bucket: str, fuuid: str, taille: int):
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

        if len(self.__batch_visites) >= self.__limite_batch:
            self.commit_visites()

    def commit_visites(self):
        batch = self.__batch_visites
        self.__batch_visites = None
        self.__cur.executemany(scripts_database.CONST_PRESENCE_FICHIERS, batch)
        self.__con.commit()

    def close(self):
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

        self.__path_database.parent.mkdir(parents=True, exist_ok=True)
        con = self.ouvrir_database()
        con.execute(scripts_database.CONST_CREATE_FICHIERS)
        con.close()

    async def run(self):
        raise NotImplementedError('must override')

    async def stop(self):
        self._stop_store.set()

    def get_path_fuuid(self, bucket: str, fuuid: str) -> pathlib.Path:
        raise NotImplementedError('must override')

    async def consigner(self, path_src: pathlib.Path, fuuid: str):
        # Tenter d'inserer le fichier comme nouveau actif dans la base de donnees
        con = sqlite3.connect(self.__path_database, check_same_thread=True)
        stat = path_src.stat()

        date_now = datetime.datetime.now(tz=pytz.UTC)
        data = {
            'fuuid': fuuid,
            'etat_fichier': Constantes.DATABASE_ETAT_ACTIF,
            'taille': stat.st_size,
            'bucket': Constantes.BUCKET_PRINCIPAL,
            'date_presence': date_now,
            'date_verification': date_now,
            'date_reclamation': date_now,
        }
        cur = con.cursor()
        try:
            cur.execute(scripts_database.CONST_INSERT_FICHIER, data)
        except sqlite3.IntegrityError as e:
            if 'FICHIERS.fuuid' in e.args[0]:
                self.__logger.debug("ConsignationStore.consigner fuuid %s existe deja - OK" % fuuid)
                cur.execute(scripts_database.CONST_ACTIVER_SI_MANQUANT, data)
                cur.execute(scripts_database.CONST_VERIFIER_FICHIER, data)
            else:
                raise e
        finally:
            cur.close()
            con.commit()
            con.close()

        # await self.emettre_batch_visites([fuuid], verification=True)
        await self.emettre_evenement_consigne(fuuid)

    async def changer_bucket(self, fuuid: str, bucket: str):
        """ Deplace le fichier vers un bucket """
        raise NotImplementedError('must override')

    async def supprimer(self, fuuid: str):
        """ Marquer orphelin si fichier est actif """
        raise NotImplementedError('must override')

    async def recuperer(self, fuuid: str):
        """ Marquer le fichier orphelin comme actif """
        raise NotImplementedError('must override')

    async def purger(self, fuuid: str):
        """ Retirer le fichier du bucket """
        raise NotImplementedError('must override')

    async def stream_fuuid(self, fuuid: str, response: web.StreamResponse, start: Optional[int] = None, end: Optional[int] = None):
        """ Stream les bytes du fichier en utilisant la response """
        raise NotImplementedError('must override')

    async def entretien(self):
        """ Declenche un entretien (visite, verification, purge, etc.) """
        raise NotImplementedError('must override')

    async def visiter_fuuids(self):
        """ Visiter tous les fichiers presents, s'assurer qu'ils sont dans la base de donnees. """
        raise NotImplementedError('must override')

    async def conserver_backup(self, fichier_temp: tempfile.TemporaryFile, uuid_backup: str, domaine: str,
                               nom_fichier: str):
        raise NotImplementedError('must override')

    async def rotation_backups(self, uuid_backups_conserver: list[str]):
        """ Supprime tous les backups qui ne sont pas dans la liste """
        raise NotImplementedError('must override')

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

    def get_info_fichier(self, fuuid: str):
        con = self.ouvrir_database()
        cur = con.cursor()
        cur.execute(scripts_database.CONST_INFO_FICHIER, {'fuuid': fuuid})
        _fuuid, etat_fichier, taille, bucket, date_presence, date_verification, date_reclamation = cur.fetchone()
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

    async def emettre_batch_visites(self, fuuids: list[str], verification=False):
        producer = self._etat.producer
        await asyncio.wait_for(producer.producer_pret().wait(), timeout=10)

        message = {
            'fuuids': fuuids,
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


class ConsignationStoreMillegrille(ConsignationStore):

    def __init__(self, etat: EtatFichiers):
        super().__init__(etat)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

    def get_path_fuuid(self, bucket: str, fuuid: str) -> pathlib.Path:
        dir_consignation = pathlib.Path(self._etat.configuration.dir_consignation)
        sub_folder = fuuid[-2:]
        path_fuuid = pathlib.Path(dir_consignation, Constantes.DIR_BUCKETS, bucket, sub_folder, fuuid)
        return path_fuuid

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

    async def thread_visiter(self):
        stop_coro = self._stop_store.wait()
        await asyncio.wait([stop_coro], timeout=30)
        while self._stop_store.is_set() is False:
            self.__logger.info("thread_visiter Debut visiter fuuids")
            try:
                await asyncio.to_thread(self.visiter_fuuids)
            except Exception:
                self.__logger.exception('thread_visiter Erreur visite fuuids')
            await asyncio.wait([stop_coro], timeout=Constantes.CONST_INTERVALLE_VISITE_MILLEGRILLE)

    async def run(self):
        await asyncio.gather(
            self.thread_visiter()
        )

    async def changer_bucket(self, fuuid: str, bucket: str):
        raise NotImplementedError('todo')

    async def supprimer(self, fuuid: str):
        raise NotImplementedError('todo')

    async def purger(self, fuuid: str):
        raise NotImplementedError('todo')

    async def stream_fuuid(
            self, fuuid: str, response: web.StreamResponse,
            start: Optional[int] = None, end: Optional[int] = None
    ):
        # Pour local FS, ignore la base de donnes. On verifie si le fichier existe dans actif ou archives
        path_fichier = self.get_path_fuuid(Constantes.BUCKET_PRINCIPAL, fuuid)
        if path_fichier.exists() is False:
            path_fichier = self.get_path_fuuid(Constantes.BUCKET_ARCHIVES, fuuid)
            if path_fichier.exists() is False:
                raise Exception('fichier inconnu %s' % fuuid)

        with path_fichier.open(mode='rb') as input_file:
            if start is not None and start > 0:
                input_file.seek(start, 0)
                position = start
            else:
                position = 0
            for chunk in input_file:
                if end is not None and position + len(chunk) > end:
                    taille_chunk = end - position + 1
                    await response.write(chunk[:taille_chunk])
                    break  # Termine
                else:
                    await response.write(chunk)
                position += len(chunk)

    def visiter_fuuids(self):
        dir_buckets = pathlib.Path(self._etat.configuration.dir_consignation, Constantes.DIR_BUCKETS)
        entretien_db = EntretienDatabase(self._etat)

        try:
            # Parcourir tous les buckets recursivement (depth-first)
            for bucket in dir_buckets.iterdir():
                self.visiter_bucket(bucket.name, bucket, entretien_db)

            entretien_db.commit_visites()
        finally:
            entretien_db.close()

    def visiter_bucket(self, bucket: str, path_repertoire: pathlib.Path, entretien_db: EntretienDatabase):
        for item in path_repertoire.iterdir():
            if item.is_dir():
                # Parcourir recursivement
                self.visiter_bucket(bucket, item, entretien_db)
            elif item.is_file():
                stat = item.stat()
                entretien_db.ajouter_visite(bucket, item.name, stat.st_size)

    async def conserver_backup(self, fichier_temp: tempfile.TemporaryFile, uuid_backup: str, domaine: str,
                               nom_fichier: str):
        path_backup = pathlib.Path(self._etat.configuration.dir_consignation, Constantes.DIR_BACKUP, uuid_backup, domaine)
        await asyncio.to_thread(self.__conserver_backup, fichier_temp, path_backup, nom_fichier)

    def __conserver_backup(self, fichier_temp: tempfile.TemporaryFile, repertoire: pathlib.Path, nom_fichier):
        fichier_temp.seek(0)
        path_fichier_fichier = pathlib.Path(repertoire, nom_fichier)
        path_fichier_work = pathlib.Path(repertoire, '%s.work' % nom_fichier)
        repertoire.mkdir(parents=True, exist_ok=True)
        with open(path_fichier_work, 'wb') as fichier:
            while True:
                chunk = fichier_temp.read(64*1024)
                if not chunk:
                    break
                fichier.write(chunk)

        # Renommer fichier (retrirer .work)
        path_fichier_work.rename(path_fichier_fichier)

    async def rotation_backups(self, uuid_backups_conserver: list[str]):
        """ Supprime tous les backups qui ne sont pas dans la liste """
        dir_consignation = pathlib.Path(self._etat.configuration.dir_consignation, Constantes.DIR_BACKUP)
        for item in dir_consignation.iterdir():
            if item.is_dir():
                if item.name not in uuid_backups_conserver:
                    self.__logger.info("Supprimer repertoire de backup %s" % item.name)
                    shutil.rmtree(item)


def map_type(type_store: str) -> Type[ConsignationStore]:
    if type_store == Constantes.TYPE_STORE_MILLEGRILLE:
        return ConsignationStoreMillegrille
    elif type_store == Constantes.TYPE_STORE_SFTP:
        raise NotImplementedError()
    elif type_store == Constantes.TYPE_STORE_AWSS3:
        raise NotImplementedError()
    else:
        raise Exception('Type %s non supporte' % type_store)