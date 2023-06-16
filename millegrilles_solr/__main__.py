import argparse
import asyncio
import logging

from solrdao import SolrDao

logger = logging.getLogger(__name__)


class ESMain:

    def __init__(self):
        self.__solrdao = SolrDao()

    async def configurer(self):
        self.__solrdao.configure(
            '/var/opt/millegrilles/secrets/pki.core.cert',
            '/var/opt/millegrilles/secrets/pki.core.cle',
            '/var/opt/millegrilles/configuration/pki.millegrille.cert'
        )

        # Configurer core1
        await self.__solrdao.initialiser_solr()

    async def run(self):
        await self.__solrdao.ping()

        # Debug
        #await self.__solrdao.list_field_types()
        #await self.__solrdao.preparer_sample_data()
        await self.__solrdao.requete()


def parse_args() -> argparse.Namespace:
    pass


async def demarrer():
    logger.info("Configurer main millegrilles_solr")
    args = parse_args()
    main = ESMain()
    await main.configurer()
    logger.info("Run main millegrilles_solr")
    await main.run()
    logger.info("Fin main millegrilles_solr")


if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger('__main__').setLevel(logging.INFO)
    logging.getLogger('solrdao').setLevel(logging.DEBUG)
    asyncio.run(demarrer())

