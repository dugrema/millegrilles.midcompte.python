import asyncio
import datetime
import logging
import subprocess
import tarfile
import json

from os import listdir, path, unlink, makedirs
from typing import Optional

from millegrilles_messages.messages.EnveloppeCertificat import EnveloppeCertificat
from millegrilles_utils.Configuration import ConfigurationBackup
from millegrilles_messages.chiffrage.Mgs3 import CipherMgs3

TAILLE_BUFFER = 32 * 1024


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
        if self.__enveloppe_ca is None:
            raise ValueError('enveloppe_ca doit etre initialise')

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

        # Creer repertoire archives
        path_archives = path.join(self.__source, '_ARCHIVES')
        makedirs(path_archives, mode=0o755, exist_ok=True)

        path_tar_file = path.join(path_archives, '%s.%s.tar' % (repertoire, date_courante))
        nom_archive_chiffree = '%s.mgs3' % nom_archive
        path_catalogue = path.join(self.__source, 'catalogue.json')
        fichier_dest = path.join(self.__source, nom_archive_chiffree)

        try:
            # Chiffrer le .tar.xz
            fichier_dest, info_chiffrage = await self.chiffrer_archive(nom_archive, fichier_dest)

            with open(path_catalogue, 'w') as fichier_cat:
                json.dump(info_chiffrage, fichier_cat)

            with tarfile.open(path_tar_file, 'w') as tar_out:
                tar_out.add(path_catalogue, arcname='catalogue.json')
                tar_out.add(fichier_dest, arcname=nom_archive_chiffree)
        except IOError:
            # Cleanup fichier tar si present
            unlink(path_tar_file)
        finally:
            # Cleanup
            unlink(fichier_dest)
            unlink(path_catalogue)

    async def chiffrer_archive(self, nom_archive: str, fichier_dest: str):
        public_x25519 = self.__enveloppe_ca.get_public_x25519()
        cipher = CipherMgs3(public_x25519)
        fichier_src = path.join(self.__source, nom_archive)

        with open(fichier_dest, 'wb') as fp_dest:
            with open(fichier_src, 'rb') as fp_src:
                bytes_buffer = fp_src.read(TAILLE_BUFFER)
                while len(bytes_buffer) > 0:
                    output_buffer = cipher.update(bytes_buffer)
                    fp_dest.write(output_buffer)
                    bytes_buffer = fp_src.read(TAILLE_BUFFER)

        # Retirer fichier src (dechiffre)
        unlink(fichier_src)

        # Creer fichier d'information de chiffrage
        info_chiffrage = cipher.get_info_dechiffrage()
        return fichier_dest, info_chiffrage


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
