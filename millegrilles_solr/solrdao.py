import json
import re
import logging
from tempfile import TemporaryFile
from typing import Optional

import aiohttp
from ssl import SSLContext


class SolrDao:

    def __init__(self, etat_relaisolr):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__etat_relaisolr = etat_relaisolr

        self.__ssl_context: Optional[SSLContext] = None
        # self.__url_solr = 'https://localhost:8983'

    @property
    def nom_collection_fichiers(self):
        return self.__etat_relaisolr.configuration.nom_collection_fichiers

    @property
    def solr_url(self):
        return self.__etat_relaisolr.configuration.solr_url

    def configure(self):
        config = self.__etat_relaisolr.configuration
        cert_path = config.cert_pem_path
        self.__logger.debug("configure Charger certificat %s" % cert_path)
        self.__ssl_context = SSLContext()
        self.__ssl_context.load_cert_chain(cert_path, config.key_pem_path)
        # self.__ssl_context.load_verify_locations(capath=ca_path)  # Aucun effet sur client

    async def ping(self):
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            ping_url = f'{self.solr_url}/api'
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

            reload_fichiers_url = f'{self.solr_url}/api/collections/{self.nom_collection_fichiers}'
            data = {'reload': {}}
            async with session.post(reload_fichiers_url, ssl=self.__ssl_context, json=data) as resp:
                # get_core_url = f'{self.solr_url}/solr/admin/collections/{self.nom_collection_fichiers}'
                # async with session.get(get_core_url, ssl=self.__ssl_context) as resp:
                #     self.__logger.debug("initialiser_solr Status collections : %d" % resp.status)
                if resp.status != 200:
                    self.__logger.debug("initialiser_solr Collections")
                    await self.initialiser_collection_fichiers(session)
                else:
                    resultat = await resp.json()
                    self.__logger.debug("initialiser_solr Collections : %s" % resultat)

    async def reset_index(self, nom_collection, delete=False):
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            if delete:
                # Delete collections
                delete_url = f'{self.solr_url}/api/collections/{nom_collection}'
                async with session.delete(delete_url, ssl=self.__ssl_context) as resp:
                    if resp.status != 200:
                        self.__logger.warning("reset_index Status DELETE de collections %s : %d" % (nom_collection, resp.status))
                    resp.raise_for_status()
            else:
                # Delete data
                data = {'delete': {'query': '*:*'}}
                delete_url = f'{self.solr_url}/solr/{nom_collection}/update'
                async with session.post(delete_url, ssl=self.__ssl_context, json=data) as resp:
                    if resp.status != 200:
                        self.__logger.warning("reset_index Status DELETE de data collections %s : %d" % (nom_collection, resp.status))
                    else:
                        self.__logger.info("reset_index Status DELETE de collections OK (200)")

    async def requete(self, nom_collection, user_id, query, qf='name^2 content', start=0, limit=100):
        timeout = aiohttp.ClientTimeout(total=5)  # Timeout requete 5 secondes
        async with aiohttp.ClientSession(timeout=timeout) as session:
            requete_url = f'{self.solr_url}/solr/{nom_collection}/select'
            # params = {'q': '*:*'}
            params = {
                'defType': 'dismax',
                'fq': f'user_id:{user_id}',
                'q': query,
                'qf': qf,
                'fl': 'id,tuuid,score',
                'start': start,
                'rows': limit,
            }
            async with session.get(requete_url, ssl=self.__ssl_context, params=params) as resp:
                self.__logger.debug("requete response status : %d" % resp.status)
                resp.raise_for_status()
                return await resp.json()

    async def indexer(self, nom_collection, user_id, doc_id: str, metadata: dict, fichier: Optional[TemporaryFile]):
        if fichier is None:
            await self._indexer_document(nom_collection, user_id, doc_id, metadata)
        else:
            await self._indexer_fichier(nom_collection, user_id, doc_id, metadata, fichier)

    async def _indexer_document(self, nom_collection, user_id, doc_id: str, metadata: dict):
        data = {"id": doc_id, "user_id": [user_id]}
        data.update(metadata)
        try:
            data['name'] = filtrer_nom(data['nom'])
        except KeyError:
            pass
        timeout = aiohttp.ClientTimeout(connect=5, total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            data_update_url = f'{self.solr_url}/api/collections/{nom_collection}/update?commit=true'
            async with session.post(data_update_url, ssl=self.__ssl_context, json=data) as resp:
                self.__logger.debug("_indexer_document Ajout data status: %d" % resp.status)
                resp.raise_for_status()
                self.__logger.debug("_indexer_document Reponse : %s", await resp.json())

    async def _indexer_fichier(self, nom_collection, user_id, doc_id: str, metadata: dict, fichier: TemporaryFile):
        params = {
            'commit': 'true',
            'uprefix': 'ignored_*',
            # 'fmap.content': 'content',
            "literal.id": doc_id,
            "literal.user_id": user_id,
        }

        for k, v in metadata.items():
            params[f'literal.{k}'] = v
            if k == 'nom':
                params['literal.name'] = filtrer_nom(v)

        mimetype = metadata.get('mimetype') or 'application/octet-stream'

        timeout = aiohttp.ClientTimeout(connect=5, total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            data_update_url = f'{self.solr_url}/solr/{nom_collection}/update/extract'
            async with session.post(
                    data_update_url,
                    ssl=self.__ssl_context,
                    params=params,
                    data=fichier,
                    headers={'Content-Type': mimetype}
            ) as resp:
                self.__logger.debug("_indexer_fichier Ajout data status: %d" % resp.status)
                resp.raise_for_status()
                self.__logger.debug("_indexer_fichier Reponse : %s", await resp.json())

    async def list_field_types(self):
        async with aiohttp.ClientSession() as session:
            requete_url = f'{self.solr_url}/solr/{self.nom_collection_fichiers}/schema/fieldtypes'
            async with session.get(requete_url, ssl=self.__ssl_context) as resp:
                resp.raise_for_status()
                str_formattee = json.dumps(await resp.json(), indent=2)
                self.__logger.debug("list_field_types Reponse\n%s", str_formattee)

    async def initialiser_collection_fichiers(self, session):
        nom_collection = self.nom_collection_fichiers

        create_collection_url = f'{self.solr_url}/api/collections'
        data = {'create': {'name': nom_collection, 'numShards': 1, 'replicationFactor': 1}}
        async with session.post(create_collection_url, ssl=self.__ssl_context, json=data) as resp:
            self.__logger.debug("initialiser_solr Resultat creer %s : %d" % (nom_collection, resp.status))
            resp.raise_for_status()
            self.__logger.debug("initialiser_solr Reponse : %s", await resp.json())

        config_extract_url = f'{self.solr_url}/api/collections/{self.nom_collection_fichiers}/config'
        config_extract_data = {
            "add-requesthandler": {
                'name': '/update/extract',
                'class': 'solr.extraction.ExtractingRequestHandler',
                'defaults': {"lowernames": "true", "captureAttr": "false"},
            }
        }
        async with session.post(config_extract_url, ssl=self.__ssl_context, json=config_extract_data) as resp:
            self.__logger.debug("initialiser_solr Resultat config extract %s : %d" % (nom_collection, resp.status))
            resp.raise_for_status()

        schema_url = f'{self.solr_url}/api/collections/{nom_collection}/schema'
        data = {'add-field': [
            {"name": "name", "type": "text_en_splitting_tight", "stored": False},
            {"name": "content", "type": "text_general", "stored": False},
            {"name": "user_id", "type": "string", "multiValued": True, "stored": True},
            {"name": "tuuid", "type": "string", "stored": True},
            # {"name": "series_t", "type": "text_en_splitting_tight", "multiValued": False},
            # {"name": "cat", "type": "string", "multiValued": True},
            # {"name": "manu", "type": "string"},
            # {"name": "features", "type": "text_general", "multiValued": True},
            # {"name": "weight", "type": "pfloat"},
            # {"name": "price", "type": "pfloat"},
            # {"name": "popularity", "type": "pint"},
            # {"name": "inStock", "type": "boolean", "stored": True},
            # {"name": "store", "type": "location"},
        ]}
        async with session.post(schema_url, ssl=self.__ssl_context, json=data) as resp:
            self.__logger.debug("initialiser_solr Resultat schema : %d" % resp.status)
            resp.raise_for_status()
            self.__logger.debug("initialiser_solr Reponse : %s", await resp.json())

        commit_url = f'{self.solr_url}/api/collections/{nom_collection}/config'
        data = {
            "set-property": {
                "updateHandler.autoCommit.maxTime": 15000,
            }
        }
        async with session.post(commit_url, ssl=self.__ssl_context, json=data) as resp:
            self.__logger.debug("initialiser_solr Commit data test : %d" % resp.status)
            resp.raise_for_status()
            self.__logger.debug("initialiser_solr Commit reponse : %s", await resp.json())

    async def preparer_sample_data(self):
        data = [
            {
                "id": "978-0641723445",
                "user_id": "user1",
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
                "user_id": ["user1", "user2"],
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
            data_update_url = f'{self.solr_url}/api/collections/{self.nom_collection_fichiers}/update?commit=true'
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

    async def preparer_sample_file(self):
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            data_update_url = f'{self.solr_url}/solr/{self.nom_collection_fichiers}/update/extract'
            with open('/home/mathieu/tas/tmp/test2.pdf', 'rb') as file:
                params = {
                    'commit': 'true',
                    'uprefix': 'ignored_*',
                    # 'fmap.content': 'content',
                    'literal.id': 'pdf5',
                    'literal.name': 'Fichier PDF de test 5.pdf',
                    'literal.user_id': 'user1',
                }
                async with session.post(
                    data_update_url,
                    ssl=self.__ssl_context,
                    params=params,
                    data=file,
                    headers={'Content-Type': 'application/octet-stream'}
                ) as resp:
                    self.__logger.debug("preparer_sample_data Ajout fichier PDF test : %d" % resp.status)
                    resp.raise_for_status()
                    self.__logger.debug("preparer_sample_data Reponse ajout pdf : %s", await resp.json())


def filtrer_nom(nom) -> str:
    val = re.sub('[\\W]+', ' ', nom)
    # nom = ''.join(s for s in nom if s.isalnum())
    # nom = nom.replace('-', ' ')
    val = val.replace('_', ' ')
    # val = val.replace('.', ' ')
    # nom = nom.replace('\'', ' ')
    # nom = nom.replace('’', ' ')
    return val
