import errno
import logging
import pathlib
import shutil

from typing import Type

from millegrilles_fichiers import Constantes
from millegrilles_fichiers.EtatFichiers import EtatFichiers


class ConsignationStore:

    def __init__(self, etat: EtatFichiers):
        self._etat = etat
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

    def get_path_actif(self, fuuid: str) -> pathlib.Path:
        raise NotImplementedError('must override')

    def get_path_archive(self, fuuid: str) -> pathlib.Path:
        raise NotImplementedError('must override')

    async def consigner(self, path_src: pathlib.Path, fuuid: str):
        raise NotImplementedError('must override')

    async def archiver(self, path_src: pathlib.Path, fuuid: str):
        raise NotImplementedError('must override')

    async def supprimer(self, path_src: pathlib.Path, fuuid: str):
        raise NotImplementedError('must override')


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
