import asyncio
import logging
import os
import pysftp

from typing import Optional
from pysftp import CnOpts
from stat import S_ISDIR

from millegrilles_fichiers.EtatFichiers import EtatFichiers
from millegrilles_fichiers.SQLiteDao import SQLiteReadOperations, SQLiteWriteOperations, SQLiteBatchOperations


class ConsignationBackup:

    def __init__(self, stop_event: asyncio.Event, etat_instance: EtatFichiers, consignation_handler):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__stop_event = stop_event
        self.__etat_instance = etat_instance
        self.__consignation_handler = consignation_handler

        self.__backup_store: Optional[BackupStore] = None
        self.__backup_pret = asyncio.Event()

    async def run(self):
        await asyncio.gather(
            self.entretien(),
            self.__run_backup(),
            self.__attendre_fermer(),
        )

    async def __run_backup(self):
        """
        Thread d'execution du store de backup
        :return:
        """
        while self.__stop_event.is_set() is False:
            await self.__backup_pret.wait()
            if self.__stop_event.is_set():
                return  # Stopped

            if self.__backup_store is not None:
                await self.__backup_store.run_backup()

            # Empecher d'exuter le backup immediatement apres un arret
            self.__backup_pret.clear()

    async def __attendre_fermer(self):
        await self.__stop_event.wait()

        # Cleanup de fermeture
        self.__backup_pret.set()  # Debloquer thread run backup

    async def entretien(self):
        while self.__stop_event.is_set() is False:

            self.__logger.debug("Consignation backup run")

            # Declencher un backup
            self.__backup_pret.set()

            try:
                await asyncio.wait_for(self.__stop_event.wait(), 30)
            except asyncio.TimeoutError:
                pass

    async def changement_topologie(self):
        topologie = self.__etat_instance.topologie
        type_backup = topologie.get('type_backup')

        if type_backup == 'sftp':
            await self.configurer_sftp()
        else:
            await self.desactiver_backup()

    async def configurer_sftp(self):
        if isinstance(self.__backup_store, BackupStoreSftp):
            pass
        else:
            if self.__backup_store is not None:
                await self.__backup_store.fermer()
            self.__backup_store = BackupStoreSftp(self.__etat_instance, self.__consignation_handler)

        await self.__backup_store.configurer()  # Reconfigurer

    async def desactiver_backup(self):
        if self.__backup_store is not None:
            await self.__backup_store.fermer()


class BackupStore:

    def __init__(self, etat_instance: EtatFichiers, consignation_handler):
        self._etat_instance = etat_instance
        self._consignation_handler = consignation_handler

    async def configurer(self):
        raise NotImplementedError('not implemented')

    async def fermer(self):
        raise NotImplementedError('not implemented')

    async def run_backup(self):
        raise NotImplementedError('not implemented')

    async def get_batch_fuuids(self):
        """
        Recupere une batch de fuuids presents dans la consignation a transferer vers le backup sftp
        :return:
        """
        with self._etat_instance.sqlite_connection() as connection:
            async with SQLiteReadOperations(connection) as dao_read:
                return await asyncio.to_thread(dao_read.get_backup_batch)


class BackupStoreSftp(BackupStore):
    """
    Supporte le backup d'une consignation vers sftp.
    """

    def __init__(self, etat_instance: EtatFichiers, consignation_handler):
        super().__init__(etat_instance, consignation_handler)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

    async def configurer(self):
        pass

    async def fermer(self):
        pass

    async def run_backup(self):
        batch_fuuids = await self.get_batch_fuuids()
        self.__logger.debug("run_backup Backup fuuids %s" % batch_fuuids)

        configuration = self._etat_instance.topologie

        hostname = configuration['hostname_sftp_backup']
        username = configuration['username_sftp_backup']
        port = configuration.get('port_sftp_backup') or 22
        remote_path_sftp = configuration['remote_path_sftp_backup']

        private_key_path = '/var/opt/millegrilles/secrets/passwd.fichiers_ed25519.txt'

        sftp = Sftp(hostname=hostname, port=port, username=username, private_key=private_key_path)
        await asyncio.to_thread(sftp.connect)

        for fichier in batch_fuuids:
            fuuid = fichier['fuuid']
            bucket = fichier['bucket']

            # Recuperer un fp a partir de la source
            async with self._consignation_handler.get_fp_fuuid(fuuid) as fp:
                self.__logger.debug("backup fichier %s" % fuuid)
                subfolder = os.path.join(remote_path_sftp, fuuid[-2:])
                try:
                    await asyncio.to_thread(sftp.mkdir, subfolder)
                except IOError:
                    pass  # Repertoire existe deja
                path_fichier = os.path.join(subfolder, fuuid)
                await asyncio.to_thread(sftp.putfo, fp, remotepath=path_fichier)


class Sftp:
    def __init__(self, hostname, username, private_key, port=22):
        """Constructor Method"""
        # Set connection object to None (initial value)
        self.connection = None
        self.hostname = hostname
        self.username = username
        self.private_key = private_key
        self.port = port

    def connect(self):
        """Connects to the sftp server and returns the sftp connection object"""

        try:
            # Get the sftp connection object
            cnopts = CnOpts()
            cnopts.hostkeys = None
            # cnopts.hostkeys.load('sftpserver.pub')

            self.connection = pysftp.Connection(
                host=self.hostname,
                username=self.username,
                # password=self.password,
                private_key=self.private_key,
                port=self.port,
                cnopts=cnopts,
            )
        except Exception as err:
            raise Exception(err)
        finally:
            print(f"Connected to {self.hostname} as {self.username}.")

    def disconnect(self):
        """Closes the sftp connection"""
        self.connection.close()
        print(f"Disconnected from host {self.hostname}")

    def listdir(self, remote_path):
        """lists all the files and directories in the specified path and returns them"""
        for obj in self.connection.listdir(remote_path):
            yield obj

    def listdir_attr(self, remote_path):
        """lists all the files and directories (with their attributes) in the specified path and returns them"""
        for attr in self.connection.listdir_attr(remote_path):
            yield attr

    def is_dir(self, file_stat):
        return S_ISDIR(file_stat.st_mode)

    def upload(self, source_local_path, remote_path):
        """
        Uploads the source files from local to the sftp server.
        """

        try:
            print(
                f"uploading to {self.hostname} as {self.username} [(remote path: {remote_path});(source local path: {source_local_path})]"
            )

            # Download file from SFTP
            self.connection.put(source_local_path, remote_path)
            print("upload completed")

        except Exception as err:
            raise Exception(err)

    def download(self, remote_path, target_local_path):
        """
        Downloads the file from remote sftp server to local.
        Also, by default extracts the file to the specified target_local_path
        """

        try:
            print(
                f"downloading from {self.hostname} as {self.username} [(remote path : {remote_path});(local path: {target_local_path})]"
            )

            # Create the target directory if it does not exist
            path, _ = os.path.split(target_local_path)
            if not os.path.isdir(path):
                try:
                    os.makedirs(path)
                except Exception as err:
                    raise Exception(err)

            # Download from remote sftp server to local
            self.connection.get(remote_path, target_local_path)
            print("download completed")

        except Exception as err:
            raise Exception(err)

    def parcours_fichiers_recursif(self, remote_path: str, filtre=None):
        listing = self.listdir_attr(remote_path)
        for item in listing:
            if filtre:
                if not filtre(item):
                    continue

            if self.is_dir(item):
                subpath = os.path.join(remote_path, item.filename)
                for sub_item in self.parcours_fichiers_recursif(subpath):
                    yield sub_item
            else:
                yield {"file": item, "directory": remote_path}

    def putfo(self, flo, remotepath=None, file_size=0, callback=None,
              confirm=True):
        return self.connection.putfo(flo, remotepath, file_size, callback, confirm)

    def mkdir(self, path, mode=750):
        return self.connection.mkdir(path, mode)
