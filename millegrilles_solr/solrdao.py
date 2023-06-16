import json
import logging
from typing import Optional

import aiohttp
from ssl import SSLContext


class SolrDao:

    def __init__(self):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__ssl_context: Optional[SSLContext] = None
        self.__url_solr = 'https://localhost:8983'
        self.__nom_collection = 'techproducts1'

    def configure(self, cert_path: str, key_path: str, ca_path: str):
        self.__logger.debug("configure Charger certificat %s" % cert_path)
        self.__ssl_context = SSLContext()
        self.__ssl_context.load_cert_chain(cert_path, key_path)
        # self.__ssl_context.load_verify_locations(capath=ca_path)  # Aucun effet sur client

    async def ping(self):
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            ping_url = f'{self.__url_solr}/api'
            async with session.get(ping_url, ssl=self.__ssl_context) as resp:
                self.__logger.debug("PING status : %d" % resp.status)
                resp.raise_for_status()
                await resp.read()

    async def initialiser_solr(self):
        """
        Creer core initial, templates, etc.
        :return:
        """
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:

            get_core_url = f'{self.__url_solr}/api/collections/{self.__nom_collection}'
            async with session.get(get_core_url, ssl=self.__ssl_context) as resp:
                self.__logger.debug("initialiser_solr Status collections : %d" % resp.status)
                resultat = await resp.json()
                self.__logger.debug("initialiser_solr Collections : %s" % resultat)

            if resp.status != 200:
                create_collection_url = f'{self.__url_solr}/api/collections'
                data = {'create': {'name': self.__nom_collection, 'numShards': 1, 'replicationFactor': 1}}
                async with session.post(create_collection_url, ssl=self.__ssl_context, json=data) as resp:
                    self.__logger.debug("initialiser_solr Resultat creer techproducts : %d" % resp.status)
                    resp.raise_for_status()
                    self.__logger.debug("initialiser_solr Reponse : %s", await resp.json())

                schema_url = f'{self.__url_solr}/api/collections/{self.__nom_collection}/schema'
                data = {'add-field': [
                    {"name": "name", "type": "text_en_splitting_tight", "multiValued": False},
                    {"name": "series_t", "type": "text_en_splitting_tight", "multiValued": False},
                    {"name": "cat", "type": "string", "multiValued": True},
                    {"name": "manu", "type": "string"},
                    {"name": "features", "type": "text_general", "multiValued": True},
                    {"name": "weight", "type": "pfloat"},
                    {"name": "price", "type": "pfloat"},
                    {"name": "popularity", "type": "pint"},
                    {"name": "inStock", "type": "boolean", "stored": True},
                    {"name": "store", "type": "location"}
                ]}
                async with session.post(schema_url, ssl=self.__ssl_context, json=data) as resp:
                    self.__logger.debug("initialiser_solr Resultat schema : %d" % resp.status)
                    resp.raise_for_status()
                    self.__logger.debug("initialiser_solr Reponse : %s", await resp.json())

                commit_url = f'{self.__url_solr}/api/collections/{self.__nom_collection}/config'
                data = {"set-property": {"updateHandler.autoCommit.maxTime": 15000}}
                async with session.post(commit_url, ssl=self.__ssl_context, json=data) as resp:
                    self.__logger.debug("initialiser_solr Commit data test : %d" % resp.status)
                    resp.raise_for_status()
                    self.__logger.debug("initialiser_solr Commit reponse : %s", await resp.json())

                # TODO Retirer sample data
                await self.preparer_sample_data()

    async def preparer_sample_data(self):
        data = [
            {
                "id": "978-0641723445",
                "cat": ["book", "hardcover"],
                "name": "The Lightning Thief",
                "author": "Rick Riordan",
                "series_t": "Percy Jackson and the Olympians",
                "sequence_i": 2,
                "genre_s": "fantasy",
                "inStock": True,
                "price": 12.50,
                "pages_i": 384
            }, {
                "id": "978-1423103349",
                "cat": ["book", "paperback"],
                "name": "The Sea of Monsters",
                "author": "Rick Riordan",
                "series_t": "Percy Jackson and the Olympians",
                "sequence_i": 3,
                "genre_s": "fantasy",
                "inStock": True,
                "price": 6.49,
                "pages_i": 304
            }
        ]

        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            data_update_url = f'{self.__url_solr}/api/collections/{self.__nom_collection}/update?commit=true'
            for datum in data:
                async with session.post(data_update_url, ssl=self.__ssl_context, json=datum) as resp:
                    self.__logger.debug("preparer_sample_data Ajout data test : %d" % resp.status)
                    resp.raise_for_status()
                    self.__logger.debug("preparer_sample_data Reponse : %s", await resp.json())

            # data_commit_url = f'{self.__url_solr}/api/collections/{self.__nom_collection}/commit'
            # async with session.post(data_commit_url, ssl=self.__ssl_context) as resp:
            #     self.__logger.debug("preparer_sample_data Commit : %d" % resp.status)
            #     resp.raise_for_status()
            #     self.__logger.debug("preparer_sample_data Commit reponse : %s", await resp.json())

    async def requete(self):
        timeout = aiohttp.ClientTimeout(total=5)  # Timeout requete 5 secondes
        async with aiohttp.ClientSession(timeout=timeout) as session:
            requete_url = f'{self.__url_solr}/solr/{self.__nom_collection}/select'
            params = {
                'defType': 'dismax',
                # 'q': 'monsters book lightning',
                'q': 'sea',
                'qf': 'name cat^2 author^2 series_t^2',
                'fl': '*,score'
            }
            async with session.get(requete_url, ssl=self.__ssl_context, params=params) as resp:
                self.__logger.debug("requete Ajout data test : %d" % resp.status)
                resp.raise_for_status()
                self.__logger.debug("requete Reponse : %s", await resp.json())

    async def list_field_types(self):
        async with aiohttp.ClientSession() as session:
            requete_url = f'{self.__url_solr}/solr/{self.__nom_collection}/schema/fieldtypes'
            async with session.get(requete_url, ssl=self.__ssl_context) as resp:
                resp.raise_for_status()
                str_formattee = json.dumps(await resp.json(), indent=2)
                self.__logger.debug("list_field_types Reponse\n%s", str_formattee)
