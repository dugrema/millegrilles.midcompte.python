import logging

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

    async def run(self):
        pass


async def main(source: str, dest: str, ca: Optional[str]):
    config = dict()
    if ca is not None:
        config['CA_PEM'] = ca

    generateur = GenerateurBackup(config, source, dest)
    generateur.preparer_chiffrage()
    await generateur.run()
