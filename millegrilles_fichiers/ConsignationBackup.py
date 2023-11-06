import asyncio
import logging

from typing import Optional

from millegrilles_fichiers.EtatFichiers import EtatFichiers


class ConsignationBackup:

    def __init__(self, stop_event: asyncio.Event, etat_instance: EtatFichiers):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__stop_event = stop_event
        self.__etat_instance = etat_instance

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
            self.__backup_store = BackupStoreSftp()

        await self.__backup_store.configurer()  # Reconfigurer

    async def desactiver_backup(self):
        if self.__backup_store is not None:
            await self.__backup_store.fermer()


class BackupStore:

    def __init__(self):
        pass

    async def configurer(self):
        raise NotImplementedError('not implemented')

    async def fermer(self):
        raise NotImplementedError('not implemented')

    async def run_backup(self):
        raise NotImplementedError('not implemented')


class BackupStoreSftp(BackupStore):
    """
    Supporte le backup d'une consignation vers sftp.
    """

    async def configurer(self):
        pass

    async def fermer(self):
        pass

    async def run_backup(self):
        pass
