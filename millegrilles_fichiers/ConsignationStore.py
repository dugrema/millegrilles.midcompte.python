import datetime
import errno
import logging
import pathlib
import shutil
import sqlite3

from typing import Type

import pytz

from millegrilles_fichiers import Constantes
from millegrilles_fichiers.EtatFichiers import EtatFichiers
import millegrilles_fichiers.DatabaseScripts as scripts_database


class ConsignationStore:

    def __init__(self, etat: EtatFichiers):
        self._etat = etat
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

        # Creer repertoire data, path database
        path_database = pathlib.Path(
            self._etat.configuration.dir_consignation, Constantes.DIR_DATA, Constantes.FICHIER_DATABASE)
        self.__path_database = path_database

    def get_path_actif(self, fuuid: str) -> pathlib.Path:
        raise NotImplementedError('must override')

    def get_path_archive(self, fuuid: str) -> pathlib.Path:
        raise NotImplementedError('must override')

    async def consigner(self, path_src: pathlib.Path, fuuid: str):
        # Tenter d'inserer le fichier comme nouveau actif dans la base de donnees
        con = sqlite3.connect(self.__path_database, check_same_thread=True)
        stat = path_src.stat()

        date_now = datetime.datetime.now(tz=pytz.UTC)
        data = {
            'fuuid': fuuid,
            'taille': stat.st_size,
            'etat_fichier': Constantes.DATABASE_ETAT_ACTIF,
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

    async def archiver(self, path_src: pathlib.Path, fuuid: str):
        raise NotImplementedError('must override')

    async def supprimer(self, path_src: pathlib.Path, fuuid: str):
        raise NotImplementedError('must override')

    def ouvrir_database(self, ro=False) -> sqlite3.Connection:
        return sqlite3.connect(self.__path_database, check_same_thread=True)

    def initialiser_db(self):
        self.__path_database.parent.mkdir(parents=True, exist_ok=True)
        con = self.ouvrir_database()
        con.execute(scripts_database.CONST_CREATE_FICHIERS)
        con.close()

    def get_stats(self):
        con = self.ouvrir_database()
        cur = con.cursor()
        cur.execute(scripts_database.CONST_STATS_FICHIERS)
        resultats = cur.fetchmany(4)  # Il y a 4 types de classements (actif, archive, orphelin, manquant)
        cur.close()
        con.close()

        resultats_dict = dict()
        for row in resultats:
            etat_fichier = '%ss' % row[0]  # Ajouter s a la fin de l'etat (e.g. actif devient actifs)
            resultats_dict[etat_fichier] = {
                'nombre': row[1],
                'taille': row[2]
            }

        return resultats_dict


class ConsignationStoreMillegrille(ConsignationStore):

    def __init__(self, etat: EtatFichiers):
        super().__init__(etat)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

    def get_path_actif(self, fuuid) -> pathlib.Path:
        dir_consignation = pathlib.Path(self._etat.configuration.dir_consignation)
        sub_folder = fuuid[-2:]
        path_fuuid = pathlib.Path(dir_consignation, Constantes.DIR_ACTIFS, sub_folder, fuuid)
        return path_fuuid

    def get_path_archives(self, fuuid) -> pathlib.Path:
        dir_consignation = pathlib.Path(self._etat.configuration.dir_consignation)
        sub_folder = fuuid[-2:]
        path_fuuid = pathlib.Path(dir_consignation, Constantes.DIR_ARCHIVES, sub_folder, fuuid)
        return path_fuuid

    async def consigner(self, path_src: pathlib.Path, fuuid: str):
        await super().consigner(path_src, fuuid)

        path_dest = self.get_path_actif(fuuid)
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

    async def archiver(self, path_src: pathlib.Path, fuuid: str):
        pass

    async def supprimer(self, path_src: pathlib.Path, fuuid: str):
        pass


def map_type(type_store: str) -> Type[ConsignationStore]:
    if type_store == Constantes.TYPE_STORE_MILLEGRILLE:
        return ConsignationStoreMillegrille
    elif type_store == Constantes.TYPE_STORE_SFTP:
        raise NotImplementedError()
    elif type_store == Constantes.TYPE_STORE_AWSS3:
        raise NotImplementedError()
    else:
        raise Exception('Type %s non supporte' % type_store)
