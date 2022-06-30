import asyncio
import getpass
import logging
import json
import tarfile

from os import path, makedirs, unlink
from typing import Optional

from millegrilles_messages.messages.ValidateurMessage import ValidateurMessage, ValidateurCertificatCache
from millegrilles_messages.messages.EnveloppeCertificat import EnveloppeCertificat
from millegrilles_messages.messages.CleCertificat import CleCertificat
from millegrilles_messages.messages.FormatteurMessages import SignateurTransactionSimple, FormatteurMessageMilleGrilles
from millegrilles_utils.Configuration import ConfigurationBackup
from millegrilles_messages.chiffrage.Mgs3 import DecipherMgs3

from millegrilles_messages.messages.MessagesThread import MessagesThread
from millegrilles_messages.messages.MessagesModule import RessourcesConsommation, ExchangeConfiguration


PATH_RESTAURATION = '_RESTAURATION'
TAILLE_BUFFER = 32 * 1204


class RestaurateurArchives:

    def __init__(self, config: dict, archive: str, transactions: bool, work_path: str, clecert_ca: CleCertificat):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__config = ConfigurationBackup()
        self.__archive = archive
        self.__transactions = transactions
        self.__work_path = work_path
        self.__clecert_ca = clecert_ca

        self.__enveloppe_ca: Optional[EnveloppeCertificat] = None
        self.__formatteur: Optional[FormatteurMessageMilleGrilles] = None
        self.__validateur_certificats: Optional[ValidateurCertificatCache] = None
        self.__validateur_messages: Optional[ValidateurMessage] = None

        # Parse configuration environnement
        self.__config.parse_config(config)

        self.__restaurateur_transactions: Optional[RestaurateurTransactions] = None

    def preparer_dechiffrage(self):
        path_ca = self.__config.ca_pem_path
        try:
            self.__enveloppe_ca = EnveloppeCertificat.from_file(path_ca)
        except FileNotFoundError:
            self.__logger.warning("Chiffrage annule, CA introuvable (path %s)", path_ca)
            return

        clecert = CleCertificat.from_files(self.__config.key_pem_path, self.__config.cert_pem_path)

        signateur = SignateurTransactionSimple(clecert)
        self.__formatteur = FormatteurMessageMilleGrilles(self.__enveloppe_ca.idmg, signateur)
        self.__validateur_certificats = ValidateurCertificatCache(self.__enveloppe_ca)
        self.__validateur_messages = ValidateurMessage(self.__validateur_certificats)

    async def preparer_mq(self):
        self.__restaurateur_transactions = RestaurateurTransactions(self.__config)
        await self.__restaurateur_transactions.preparer()

    async def run(self):
        if self.__archive is not None:
            self.__logger.info("Traiter archive %s" % self.__archive)
            path_archive = await self.extraire_archive()
            path_archive_dechiffree = await self.dechiffrer(path_archive)
            await self.extraire_archive_dechiffree(path_archive_dechiffree)

        if self.__restaurateur_transactions is not None:
            self.__logger.info("Traiter transactions")
            await self.__restaurateur_transactions.run()

    async def extraire_archive(self) -> str:
        makedirs(self.__work_path, mode=0o755, exist_ok=True)

        path_archive = None

        with tarfile.open(self.__archive, 'r') as fichier_tar:
            tar_member = fichier_tar.next()
            while tar_member is not None:
                if tar_member.name != 'catalogue.json':
                    path_archive = path.join(self.__work_path, tar_member.name)

                fichier_tar.extract(tar_member, self.__work_path)
                tar_member = fichier_tar.next()

        return path_archive

    async def dechiffrer(self, path_archive: str) -> str:
        catalogue_path = path.join(self.__work_path, 'catalogue.json')
        with open(catalogue_path, 'r') as fichier:
            catalogue = json.load(fichier)

        enveloppe = await self.__validateur_messages.verifier(catalogue, utiliser_date_message=True)

        cle_dechiffree = self.__clecert_ca.dechiffrage_asymmetrique(catalogue['cle'])
        decipher = DecipherMgs3(cle_dechiffree, catalogue['iv'], catalogue['tag'])

        path_archive_dechiffree = '.'.join(path_archive.split('.')[:-1])
        with open(path_archive_dechiffree, 'wb') as fichier_output:
            with open(path_archive, 'rb') as fichier:
                buffer_bytes = fichier.read(TAILLE_BUFFER)
                while len(buffer_bytes) > 0:
                    data = decipher.update(buffer_bytes)
                    fichier_output.write(data)
                    buffer_bytes = fichier.read(TAILLE_BUFFER)

        decipher.finalize()
        print("Dechiffrage OK")

        unlink(path_archive)

        return path_archive_dechiffree

    async def extraire_archive_dechiffree(self, path_archive):
        with tarfile.open(path_archive, 'r') as tar_file:
            tar_file.extractall(self.__work_path)
        unlink(path_archive)


class RestaurateurTransactions:

    def __init__(self, config: ConfigurationBackup):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__config = config
        self.__stop_event: Optional[asyncio.Event] = None
        self.__messages_thread: Optional[MessagesThread] = None

    async def preparer(self):
        reply_res = RessourcesConsommation(self.traiter_reponse)
        self.__stop_event = asyncio.Event()
        messages_thread = MessagesThread(self.__stop_event)
        messages_thread.set_reply_ressources(reply_res)

        config = {
            'CERT_PEM': self.__config.cert_pem_path,
            'KEY_PEM': self.__config.key_pem_path,
            'CA_PEM': self.__config.ca_pem_path,
        }
        messages_thread.set_env_configuration(config)

        self.__messages_thread = messages_thread

    async def traiter_reponse(self, message):
        self.__logger.info("Message recu : %s" % json.dumps(message.parsed, indent=2))

    async def run(self):
        # Demarrer traitement messages
        await self.__messages_thread.start_async()

        tasks = [
            asyncio.create_task(self.__messages_thread.run_async()),
            asyncio.create_task(self.run_traitement_transactions()),
        ]

        # Execution de la loop avec toutes les tasks
        await asyncio.tasks.wait(tasks, return_when=asyncio.tasks.FIRST_COMPLETED)

    async def run_traitement_transactions(self):
        producer = self.__messages_thread.get_producer()
        self.__logger.info("Attendre MQ")
        await self.__messages_thread.attendre_pret()
        self.__logger.info("MQ pret")


def charger_cle_ca(path_cle_ca: str) -> CleCertificat:
    if path.isfile(path_cle_ca) is False:
        raise FileNotFoundError('cle CA introuvable')

    with open(path_cle_ca, 'r') as fichier:
        info_fichier = json.load(fichier)

    cert = info_fichier['racine']['certificat']
    cle = info_fichier['racine']['cleChiffree']

    print('Charger cle de MilleGrille %s pour dechiffrage' % info_fichier['idmg'])

    # Demander mot de passe (console)
    mot_de_passe = getpass.getpass(prompt="Mot de passe CA: ", stream=None)

    # Charger cle racine (dechiffree)
    try:
        clecert = CleCertificat.from_pems(cle, cert, mot_de_passe)
    except ValueError as v:
        print("Mot de passe invalide")
        raise v

    print('Cle chargee OK')

    return clecert


async def main(archive: str, work_path: str, path_cle_ca: str, transactions: bool):
    config = dict()

    try:
        clecert = charger_cle_ca(path_cle_ca)
    except ValueError:
        print("Erreur de chargement de la cle de MilleGrille")
        return exit(1)

    extracteur = RestaurateurArchives(config, archive, transactions, work_path, clecert)

    extracteur.preparer_dechiffrage()
    if transactions is True:
        await extracteur.preparer_mq()

    await extracteur.run()
