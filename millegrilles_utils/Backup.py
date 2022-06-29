import asyncio
import datetime
import logging
import subprocess
import pytz

from os import listdir, path
from typing import Optional

from millegrilles_messages.messages.EnveloppeCertificat import EnveloppeCertificat
from millegrilles_utils.Configuration import ConfigurationBackup


class GenerateurBackup:

    def __init__(self, config: dict, source: str, dest: str):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__config = ConfigurationBackup()
        self.__source = source
        self.__dest = dest

        self.__enveloppe_ca: Optional[EnveloppeCertificat] = None

        # Parse configuration environnement
        self.__config.parse_config(config)

    def preparer_chiffrage(self):
        path_ca = self.__config.ca_pem_path
        try:
            self.__enveloppe_ca = EnveloppeCertificat.from_file(path_ca)
        except FileNotFoundError:
            self.__logger.warning("Chiffrage annule, CA introuvable (path %s)", path_ca)
            return

    # def preparer_cipher(self, nom_application: str, catalogue_backup: dict, path_output: str):
    #     # Faire requete pour obtenir les cles de chiffrage
    #     domaine_action = 'MaitreDesCles.' + Constantes.ConstantesMaitreDesCles.REQUETE_CERT_MAITREDESCLES
    #     cles_chiffrage = self.__handler_requetes.requete(domaine_action)
    #     self.__logger.debug("Cles chiffrage recu : %s" % cles_chiffrage)
    #
    #     # Creer un fichier .tar.xz.mgs2 pour streamer le backup
    #     output_stream = open(path_output, 'wb')
    #
    #     heure = datetime.datetime.utcnow().strftime(BackupAgent.FORMAT_HEURE)
    #     cipher, transaction_maitredescles = self.__backup_util.preparer_cipher(
    #         catalogue_backup, cles_chiffrage, heure,
    #         nom_application=nom_application,
    #         output_stream=output_stream
    #     )
    #
    #     self.__logger.debug("Transaction maitredescles:\n%s", json.dumps(transaction_maitredescles, indent=2))
    #
    #     lzma_compressor = lzma.open(cipher, 'w')  # Pipe data vers le cipher
    #     tar_output = tarfile.open(fileobj=lzma_compressor, mode="w|")  # Pipe data vers lzma
    #
    #     return {'cipher': cipher, 'maitredescles': transaction_maitredescles, 'lzma': lzma_compressor, 'tar': tar_output}

    async def run(self):
        repertoires = await self.identifier_repertoires()

        for repertoire in repertoires:
            await self.backup_repertoire(repertoire)

        pass

    async def identifier_repertoires(self) -> list:
        repertoires_backup = set()

        for rep in listdir(self.__source):
            path_rep = path.join(self.__source, rep)
            if rep.startswith('_') is False and path.isdir(path_rep) is True:
                repertoires_backup.add(rep)

        return sorted(repertoires_backup)

    async def backup_repertoire(self, repertoire: str):
        date_courante = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        nom_archive = '%s.tar.xz' % repertoire
        commande = ['tar', 'c', '-Jf', nom_archive, repertoire]

        resultat = await asyncio.to_thread(subprocess.run, commande, cwd=self.__source)

        if resultat.returncode == 0:
            self.__logger.debug("Archive %s OK, on supprime repertoire backup" % repertoire)
            await asyncio.to_thread(subprocess.run, ['rm', '-rf', repertoire], cwd=self.__source)
        else:
            self.__logger.error("Archive %s en erreur, on skip" % repertoire)
            return

        if self.__enveloppe_ca is not None:
            # Chiffrer le .tar.xz
            nom_archive_chiffree = '%s.mgs3'
            # cipher =


class StreamTar:
    """
    Stream processor qui permet d'ajouter des fichiers dans un .tar
    """

    def __init__(self, output_path: str):
        pass

    def process_fichier(self, path_fichier: str):
        pass


async def main(source: str, dest: str, ca: Optional[str]):
    config = dict()
    if ca is not None:
        config['CA_PEM'] = ca

    generateur = GenerateurBackup(config, source, dest)
    generateur.preparer_chiffrage()
    await generateur.run()
