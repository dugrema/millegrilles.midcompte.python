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
from millegrilles_messages.messages.FormatteurMessages import SignateurTransactionSimple, FormatteurMessageMilleGrilles
from millegrilles_messages.messages.CleCertificat import CleCertificat

TAILLE_BUFFER = 32 * 1024
REP_ARCHIVES = '_ARCHIVES'


class GenerateurBackup:

    def __init__(self, config: dict, source: str, dest: str):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__config = ConfigurationBackup()
        self.__source = source
        self.__dest = dest

        self.__enveloppe_ca: Optional[EnveloppeCertificat] = None
        self.__formatteur: Optional[FormatteurMessageMilleGrilles] = None

        # Parse configuration environnement
        self.__config.parse_config(config)

    def preparer_chiffrage(self):
        path_ca = self.__config.ca_pem_path
        try:
            self.__enveloppe_ca = EnveloppeCertificat.from_file(path_ca)
        except FileNotFoundError:
            self.__logger.warning("Chiffrage annule, CA introuvable (path %s)", path_ca)
            return

        clecert = CleCertificat.from_files(self.__config.key_pem_path, self.__config.cert_pem_path)

        signateur = SignateurTransactionSimple(clecert)
        self.__formatteur = FormatteurMessageMilleGrilles(self.__enveloppe_ca.idmg, signateur)

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
        path_archives = path.join(self.__source, REP_ARCHIVES)
        makedirs(path_archives, mode=0o755, exist_ok=True)

        path_tar_file = path.join(path_archives, '%s.%s.tar' % (repertoire, date_courante))
        nom_archive_chiffree = '%s.mgs3' % nom_archive
        path_catalogue = path.join(self.__source, 'catalogue.json')
        fichier_dest = path.join(self.__source, nom_archive_chiffree)

        try:
            # Chiffrer le .tar.xz
            fichier_dest, info_chiffrage = await asyncio.to_thread(self.chiffrer_archive, nom_archive, fichier_dest)

            # Ajouter info module
            info_chiffrage['module'] = repertoire

            await asyncio.to_thread(self.sauvegarder_tar,
                                    fichier_dest, info_chiffrage, nom_archive_chiffree, path_catalogue, path_tar_file)
        except IOError:
            # Cleanup fichier tar si present
            unlink(path_tar_file)
        finally:
            # Cleanup
            unlink(fichier_dest)
            unlink(path_catalogue)

    def sauvegarder_tar(self, fichier_dest, info_chiffrage, nom_archive_chiffree, path_catalogue, path_tar_file):
        info_chiffrage_signe, uuid_transaction = self.__formatteur.signer_message(info_chiffrage)
        with open(path_catalogue, 'w') as fichier_cat:
            json.dump(info_chiffrage_signe, fichier_cat)
        with tarfile.open(path_tar_file, 'w') as tar_out:
            tar_out.add(path_catalogue, arcname='catalogue.json')
            tar_out.add(fichier_dest, arcname=nom_archive_chiffree)

    def chiffrer_archive(self, nom_archive: str, fichier_dest: str):
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

        cipher.finalize()

        # Retirer fichier src (dechiffre)
        unlink(fichier_src)

        # Creer fichier d'information de chiffrage
        info_chiffrage = cipher.get_info_dechiffrage()
        return fichier_dest, info_chiffrage


async def main(source: str, dest: str, ca: Optional[str]):
    config = dict()
    if ca is not None:
        config['CA_PEM'] = ca

    generateur = GenerateurBackup(config, source, dest)
    generateur.preparer_chiffrage()
    await generateur.run()
