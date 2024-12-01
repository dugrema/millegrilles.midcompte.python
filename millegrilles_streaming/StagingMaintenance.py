import asyncio
import datetime
import logging
import pathlib

from millegrilles_streaming.Context import StreamingContext

from millegrilles_streaming import Constantes as StreamingConstants


class StagingMaintenanceHandler:

    def __init__(self, context: StreamingContext):
        self.__logger = logging.getLogger(__name__+'.'+self.__class__.__name__)
        self.__context = context

    async def run(self):
        await self.__entretien()

    async def setup(self):
        """
        Initial run at maintaining the download folder after a restart.
        Be aggressive at deleting .dat.work files: unless there are other streaming processes with OS file locks,
        no one is working on them.
        :return:
        """
        await asyncio.to_thread(self.download_path_maintenance, self.__context.download_path, timeout=5)

    async def __entretien(self):
        while self.__context.stopping is False:
            # Run maintenance
            await asyncio.to_thread(self.download_path_maintenance, self.__context.download_path, StreamingConstants.CONST_TIMEOUT_DOWNLOAD)
            await asyncio.to_thread(self.decrypted_path_maintenance, self.__context.decrypted_path, StreamingConstants.CONST_TIMEOUT_DECHIFFRE)

            # Wait for next pass
            await self.__context.wait(300)

    def download_path_maintenance(self, path_download: pathlib.Path, timeout: int) -> list:
        dt_expiration = datetime.datetime.now() - datetime.timedelta(seconds=timeout)
        ts_expiration = dt_expiration.timestamp()

        self.__logger.debug("download_path_maintenance of %s for files older than %s", path_download, dt_expiration)

        fuuids_supprimes = list()

        # Supprimer les fichiers .dat (et .json associe)
        for fichier in path_download.iterdir():
            if fichier.match('*.dat.work'):
                fuuid = fichier.name.split('.')[0]
                self.__logger.debug("Verifier expiration fichier download %s" % str(fichier))
                stat_fichier = fichier.stat()
                if stat_fichier.st_mtime < ts_expiration:
                    self.__logger.debug("Download %s est expire, on le supprime" % fuuid)

                    fichier.unlink()

                    fichier_json = pathlib.Path(str(fichier).replace('.dat.work', '.json'))
                    try:
                        fichier_json.unlink()
                    except FileNotFoundError:
                        pass  # Ok
                    fuuids_supprimes.append(fuuid)

        # Cleanup des .json expires et orphelins
        for fichier in path_download.iterdir():
            if fichier.match('*.json'):
                fuuid = fichier.name.split('.')[0]
                stat_json = fichier.stat()
                if stat_json.st_mtime < ts_expiration:
                    # Le json est vieux - verifier s'il existe un fichier .work associe
                    fichier_work = pathlib.Path(str(fichier).replace('.json', '.dat.work'))
                    if fichier_work.exists() is False:
                        # Le fichier json n'a aucun .dat associe, on supprime
                        self.__logger.warning("Supprimer fichier json orphelin %s" % str(fichier))
                        fichier.unlink()
                        fuuids_supprimes.append(fuuid)

        return fuuids_supprimes


    def decrypted_path_maintenance(self, path_dechiffre: pathlib.Path, timeout: int):
        pass

        """
        Supprime les fichiers dechiffres qui ont ete supprimes.
        :param path_dechiffre:
        :param timeout:
        :return: Liste de fuuids qui ont ete supprimes
        """

        dt_expiration = datetime.datetime.now() - datetime.timedelta(seconds=timeout)
        ts_expiration = dt_expiration.timestamp()

        fuuids_supprimes = list()

        self.__logger.debug("decrypted_path_maintenance of %s for files older than %s", path_dechiffre, dt_expiration)

        # Supprimer les fichiers .dat (et .json associe)
        for fichier in path_dechiffre.iterdir():
            if fichier.match('*.dat'):
                self.__logger.debug("Verifier expiration fichier dechiffre %s" % str(fichier))
                stat_fichier = fichier.stat()
                if stat_fichier.st_mtime < ts_expiration:
                    # Fichier expire
                    self.__logger.debug("Fichier dechiffre est expire %s" % str(fichier))
                    fuuid = fichier.name
                    fichier.unlink()
                    fichier_json = pathlib.Path(str(fichier).replace('.dat', '.json'))
                    try:
                        fichier_json.unlink()
                    except FileNotFoundError:
                        pass  # Ok
                    fuuids_supprimes.append(fuuid)

        # Cleanup des .json orphelins
        for fichier in path_dechiffre.iterdir():
            if fichier.match('*.json'):
                fichier_dat = pathlib.Path(str(fichier).replace('.json', '.dat'))
                if fichier_dat.exists() is False:
                    self.__logger.warning("Supprimer fichier json orphelin %s" % str(fichier))
                    # Le fichier json n'a aucun .dat associe, on supprime
                    fichier.unlink()

        return fuuids_supprimes
