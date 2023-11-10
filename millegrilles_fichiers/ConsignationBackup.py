import asyncio
import datetime
import logging
import os
import pathlib

import pysftp
import pytz

from typing import Optional
from pysftp import CnOpts
from stat import S_ISDIR

from millegrilles_fichiers import Constantes as ConstantesFichiers
from millegrilles_fichiers.EtatFichiers import EtatFichiers
from millegrilles_fichiers.SQLiteDao import SQLiteConnection, SQLiteReadOperations, SQLiteWriteOperations, SQLiteDetachedBackup
from millegrilles_fichiers import DatabaseScripts as scripts_database


class ConsignationBackup:

    def __init__(self, stop_event: asyncio.Event, etat_instance: EtatFichiers, consignation_handler):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__stop_event = stop_event
        self.__etat_instance = etat_instance
        self.__consignation_handler = consignation_handler

        self.__backup_store: Optional[BackupStore] = None
        self.__backup_pret = asyncio.Event()
        self.__sync_completee = asyncio.Event()

        # Limites
        self.__intervalle_backup_secs = 1200
        self.__limite_backup_bytes = 5_000_000_000

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

            # Verifier si la synchronisation doit etre faite
            if self.__backup_store is not None:
                try:
                    if self.__sync_completee.is_set() is False:
                        # Effectuer une synchronisation des fichiers de backup
                        await self.__backup_store.run_sync()

                        # Sync completee
                        self.__sync_completee.set()

                    # Sync ok, on peut effectuer un backup
                    await self.__backup_store.run_backup(self.__limite_backup_bytes)
                except Exception:
                    self.__logger.exception("__run_backup Erreur execution")

            # Attendre le prochain declencheur pour le backup
            self.__backup_pret.clear()

    async def __attendre_fermer(self):
        await self.__stop_event.wait()

        # Cleanup de fermeture
        self.__backup_pret.set()  # Debloquer thread run backup

    async def entretien(self):

        dernier_run = None

        while self.__stop_event.is_set() is False:

            self.__logger.debug("Consignation backup run")

            if self.__consignation_handler.sync_en_cours is False:
                now = datetime.datetime.utcnow()
                if dernier_run is None or now - datetime.timedelta(seconds=self.__intervalle_backup_secs) > dernier_run:
                    # Declencher un backup
                    dernier_run = now
                    self.__backup_pret.set()

            try:
                # Utiliser cette approche parce que l'intervalle de backup peut changer (config)
                await asyncio.wait_for(self.__stop_event.wait(), 15)
            except asyncio.TimeoutError:
                pass

    async def changement_topologie(self):
        topologie = self.__etat_instance.topologie
        type_backup = topologie.get('type_backup')

        self.__sync_completee.clear()

        self.__intervalle_backup_secs = topologie.get('backup_intervalle_secs') or self.__intervalle_backup_secs
        self.__limite_backup_bytes = topologie.get('backup_limit_bytes') or self.__limite_backup_bytes

        if type_backup == 'sftp':
            await self.configurer_sftp()
        else:
            await self.desactiver_backup()

        self.declencher_backup()

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
        self.__backup_store = None

    def declencher_backup(self):
        self.__backup_pret.set()


class BackupStore:

    def __init__(self, etat_instance: EtatFichiers, consignation_handler):
        self._etat_instance = etat_instance
        self._consignation_handler = consignation_handler
        self._date_sync: Optional[datetime.datetime] = None

    async def configurer(self):
        raise NotImplementedError('not implemented')

    async def fermer(self):
        raise NotImplementedError('not implemented')

    async def run_backup(self, limite_bytes=500_000_000):
        """ Copie les fichiers vers le systeme de backup distant """
        raise NotImplementedError('not implemented')

    async def run_sync(self):
        """ Synchronise la base de donnees locale avec le systeme de backup distant """
        raise NotImplementedError('not implemented')

    async def get_batch_fuuids(self, limite_bytes: 500_000_000):
        """
        Recupere une batch de fuuids presents dans la consignation a transferer vers le backup sftp
        :return:
        """
        params = {'date_sync': self._date_sync, 'limit': 1000}
        with self._etat_instance.sqlite_connection() as connection:
            async with SQLiteReadOperations(connection) as dao_read:
                return await asyncio.to_thread(dao_read.get_backup_batch, params, limite_bytes)


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

    async def get_sftp_connection(self):
        topologie = self._etat_instance.topologie

        hostname = topologie['hostname_sftp_backup']
        username = topologie['username_sftp_backup']
        port = topologie.get('port_sftp_backup') or 22

        configuration = self._etat_instance.configuration

        key_type = topologie['key_type_sftp_backup']
        if key_type == 'ed25519':
            private_key_path = configuration.path_key_ssh_ed25519
        elif key_type == 'rsa':
            private_key_path = configuration.path_key_rsa
        else:
            raise ValueError('Type de cle non supporte : %s' % key_type)

        sftp = Sftp(hostname=hostname, port=port, username=username, private_key=private_key_path)
        await asyncio.to_thread(sftp.connect)

        return sftp

    async def run_backup(self, limite_bytes=500_000_000):
        batch_fuuids = await self.get_batch_fuuids(limite_bytes)
        self.__logger.debug("run_backup Backup fuuids %s" % batch_fuuids)

        if len(batch_fuuids) > 0:
            configuration = self._etat_instance.topologie
            remote_path_sftp = configuration['remote_path_sftp_backup']

            sftp = await self.get_sftp_connection()

            with self._etat_instance.sqlite_connection() as connection:
                for fichier in batch_fuuids:
                    fuuid = fichier['fuuid']
                    bucket = fichier['bucket']
                    taille = fichier['taille']

                    # Recuperer un fp a partir de la source
                    async with self._consignation_handler.get_fp_fuuid(fuuid) as fp:
                        self.__logger.debug("backup fichier %s" % fuuid)

                        # Creer un subfolder pour repartir les fichiers uniformement (2 derniers chars du fuuid)
                        subfolders = [
                            os.path.join(remote_path_sftp, 'buckets'),
                            os.path.join(remote_path_sftp, 'buckets', bucket),
                            os.path.join(remote_path_sftp, 'buckets', bucket, fuuid[-2:]),
                        ]
                        for sub in subfolders:
                            try:
                                await asyncio.to_thread(sftp.mkdir, sub)
                            except IOError:
                                pass  # Repertoire existe deja

                        path_fichier = os.path.join(subfolders[-1], fuuid)
                        resultat = await asyncio.to_thread(sftp.putfo, fp, remotepath=path_fichier)
                        if resultat.st_size != taille:
                            raise Exception("Erreur upload fichier backup %s (taille mismatch), abort" % fuuid)

                        # Marquer fichier comme traiter dans la DB
                        async with SQLiteWriteOperations(connection) as dao_write:
                            await asyncio.to_thread(dao_write.touch_backup_fichier, fuuid, resultat.st_size)
        else:
            self.__logger.debug("run_backup Aucuns fichiers dans la batch")

        # Verifier backup des transactions
        await self.backup_transactions()

    async def backup_transactions(self):
        sftp = await self.get_sftp_connection()
        configuration = self._etat_instance.topologie
        remote_path_sftp = configuration['remote_path_sftp_backup']
        remote_path_transactions = os.path.join(remote_path_sftp, ConstantesFichiers.DIR_BACKUP)

        # S'assurer que le repertoire distant de backup des transactions existe
        try:
            await asyncio.to_thread(sftp.mkdir, remote_path_transactions)
        except IOError:
            pass  # Repertoire existe deja

        uuid_backup_set = set()

        async for domaine_info in self._consignation_handler.get_domaines_backups():
            self.__logger.debug("backup_transactions Domaine %s" % domaine_info)
            uuid_backup = domaine_info['uuid_backup']
            domaine = domaine_info['domaine']
            fichiers = set(domaine_info['fichiers'])
            del domaine_info['fichiers']  # Cleanup memoire

            path_backup_remote_uuid = os.path.join(remote_path_transactions, uuid_backup)
            if uuid_backup not in uuid_backup_set:
                # S'assurer que le repertoire distant de backup (uuid_backup)
                try:
                    await asyncio.to_thread(sftp.mkdir, path_backup_remote_uuid)
                except IOError:
                    pass  # Repertoire existe deja

                # Conserver tous les uuid_backups connus pour cleanup a la fin
                uuid_backup_set.add(uuid_backup)

            # S'assurer que le repertoire remote existe pour le domaine
            path_backup_remote_domaine = os.path.join(path_backup_remote_uuid, domaine)
            try:
                await asyncio.to_thread(sftp.mkdir, path_backup_remote_domaine)
            except IOError:
                pass  # Repertoire existe deja

            # Parcourir le backup distant et uploader chaque fichier de transactions manquant
            for fichier_remote in await asyncio.to_thread(sftp.parcours_fichiers_recursif, path_backup_remote_domaine):
                # Retirer le fichier remote du set
                fichiers.remove(fichier_remote['file'].filename)

            self.__logger.debug("backup_transactions Backup %s domaine %s, %d fichiers manquants a uploader vers backup" % (uuid_backup, domaine, len(fichiers)))
            for fichier_local in fichiers:
                self.__logger.debug("backup_transactions Backup %s domaine %s fichier %s" % (uuid_backup, domaine, fichier_local))
                path_remote_fichier = os.path.join(path_backup_remote_domaine, fichier_local)
                async with self._consignation_handler.get_fp_backup(uuid_backup, domaine, fichier_local) as fp:
                    resultat = await asyncio.to_thread(sftp.putfo, fp, remotepath=path_remote_fichier)

        # Supprimer les uuid_backup qui n'existent plus localement
        for uuid_backup_remote in await asyncio.to_thread(sftp.listdir, remote_path_transactions):
            if uuid_backup_remote not in uuid_backup_set:
                path_uuid_remote = os.path.join(remote_path_transactions, uuid_backup_remote)
                self.__logger.info("backup_transactions Remote uuid_backup %s n'existe pas localement, supprimer" % uuid_backup_remote)
                await asyncio.to_thread(sftp.rmdirs, path_uuid_remote)

    async def run_sync(self):
        sftp = await self.get_sftp_connection()
        configuration = self._etat_instance.topologie
        remote_path_sftp = configuration['remote_path_sftp_backup']
        buckets_path = os.path.join(remote_path_sftp, 'buckets')

        date_sync = datetime.datetime.now(tz=pytz.UTC)

        path_database_consignation = pathlib.Path(self._etat_instance.get_path_data(), ConstantesFichiers.FICHIER_DATABASE_FICHIERS)
        path_database_backup = pathlib.Path(self._etat_instance.get_path_data(), ConstantesFichiers.FICHIER_DATABASE_BACKUP)
        with SQLiteConnection(path_database_backup, check_same_thread=False) as connection:
            async with SQLiteDetachedBackup(connection) as backup_dao:
                try:
                    for item in sftp.parcours_fichiers_recursif(buckets_path):
                        file = item['file']
                        fuuid = file.filename
                        size = file.st_size

                        # Marquer fichier comme traiter dans la DB
                        # Note : on utilise la taille pour s'assurer que le contenu est transfere au complet
                        await backup_dao.ajouter_backup_consignation(fuuid, size)
                except FileNotFoundError:
                    self.__logger.info("Le sous-repertoire %s n'existe pas - c'est probablement un nouveau serveur de backup" % buckets_path)

                # Derniere batch
                await backup_dao.commit_batch()

                # Attacher destination pour permettre transfert des fichiers de backup
                await backup_dao.attach_destination(path_database_consignation)

        # Conserver la date de sync - permet de trouver les fichiers qui sont absents
        self._date_sync = date_sync


class Sftp:
    def __init__(self, hostname, username, private_key, port=22):
        """Constructor Method"""
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        # Set connection object to None (initial value)
        self.connection = None
        self.hostname = hostname
        self.username = username
        self.private_key = private_key
        self.port = port

    def connect(self):
        """Connects to the sftp server and returns the sftp connection object"""

        # Get the sftp connection object
        # Note : on n'utilise pas knowhosts (~ utilise pour desactiver la liste, il n'y a pas de toggle off)
        cnopts = CnOpts(knownhosts='~')
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
        self.__logger.debug(f"Connected to {self.hostname} as {self.username}.")

    def disconnect(self):
        """Closes the sftp connection"""
        self.connection.close()
        self.__logger.debug(f"Disconnected from host {self.hostname}")

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
            self.__logger.debug(
                f"uploading to {self.hostname} as {self.username} [(remote path: {remote_path});(source local path: {source_local_path})]"
            )

            # Download file from SFTP
            self.connection.put(source_local_path, remote_path)
            self.__logger.debug("upload completed")

        except Exception as err:
            raise Exception(err)

    def download(self, remote_path, target_local_path):
        """
        Downloads the file from remote sftp server to local.
        Also, by default extracts the file to the specified target_local_path
        """

        try:
            self.__logger.debug(
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
            self.__logger.debug("download completed")

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

    def rmdirs(self, path):

        dir_paths = list()

        def rmfile(filepath):
            self.__logger.debug("rmdirs Remove filepath %s" % filepath)
            self.connection.remove(filepath)

        def rmdir(dirpath):
            self.__logger.debug("rmdirs dirpath %s" % dirpath)
            dir_paths.append(dirpath)

        self.connection.walktree(path, rmfile, rmdir, rmfile)

        # Inverser liste des repertoires pour suppression leaf en premier
        dir_paths.reverse()

        for dir_path in dir_paths:
            self.__logger.debug("rmdirs Remove dirpath %s" % dir_path)
            self.connection.rmdir(dir_path)

        # Supprimer le repertoire top-level
        self.connection.rmdir(path)
