import logging

from asyncio import TaskGroup
from aiohttp.client_exceptions import ClientResponseError
from typing import Optional, Callable, Awaitable

from millegrilles_messages.structs.Filehost import Filehost
from millegrilles_solr.Context import SolrContext
from millegrilles_solr.requetes import RequetesHandler
from millegrilles_solr.solrdao import SolrDao
from millegrilles_solr.intake import IntakeHandler


class SolrManager:

    def __init__(self, context: SolrContext, intake: IntakeHandler, solr_dao: SolrDao, requetes_handler: RequetesHandler):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__context = context
        self.__intake = intake
        self.__solr_dao = solr_dao
        self.__requetes_handler = requetes_handler

        self.__filehost_listeners: list[Callable[[Optional[Filehost]], Awaitable[None]]] = list()

    def add_filehost_listener(self, listener: Callable[[Optional[Filehost]], Awaitable[None]]):
        self.__filehost_listeners.append(listener)

    async def setup(self):
        # Configurer core1
        try:
            await self.__solr_dao.ping()
        except ClientResponseError as e:
            if e.status == 400:
                self.__logger.fatal("Erreur connexion a SOLR, certificat HTTPS rejete")
            raise e
        await self.__solr_dao.setup()

    async def run(self):
        try:
            await self.__solr_dao.ping()
        except:
            self.__logger.exception("Error connecting to solr (ping failed). Quitting.")
            self.__context.stop()
            return

        async with TaskGroup() as group:
            group.create_task(self.__reload_filehost_thread())

        self.__logger.info("run() stopping")

    async def query(self, user_id: str, params: dict) -> dict:
        return await self.__requetes_handler.traiter_requete(user_id, params)

    async def process_job(self, job: dict):
        await self.__intake.process_job(job)

    async def run_scripts(self):
        import json
        # await self.__solrdao.ping()

        # Debug
        pass
        #await self.__solrdao.list_field_types()
        #await self.__solrdao.preparer_sample_data()
        #await self.__solrdao.preparer_sample_file()
        #resultat = await self.__requetes_handler.requete_fichiers('z2i3Xjx8abNcGbqKFa5bNzR3UGJkLWUBSgn5c6yZRQW6TxtdDPE', 'abus physiques')
        # resultat = await self.__requetes_handler.requete_fichiers('z2i3XjxE6PXsVKYy6BUzAkxv7HfZHrzmKVTZsyEJvxzpmFNjtwx', '001')
        # logger.info("Resultat requete \n%s" % json.dumps(resultat, indent=2))
        # await self.__solrdao.reset_index(self.__config.nom_collection_fichiers, delete=True)

    async def __reload_filehost_thread(self):
        while self.__context.stopping is False:
            try:
                await self.reload_filehost_configuration()
                await self.__context.wait(900)
            except:
                self.__logger.exception("Error loading filehost configuration")
                await self.__context.wait(30)

    async def reload_filehost_configuration(self):
        producer = await self.__context.get_producer()
        response = await producer.request(
            dict(), 'CoreTopologie', 'getFilehostForInstance', exchange="1.public")

        try:
            filehost_response = response.parsed
            filehost_dict = filehost_response['filehost']
            filehost = Filehost.load_from_dict(filehost_dict)
            self.__context.filehost = filehost
        except:
            self.__logger.exception("Error loading filehost")
            self.__context.filehost = None

        for l in self.__filehost_listeners:
            await l(self.__context.filehost)

    async def trigger_fetch_jobs(self, delay: Optional[float] = None):
        if delay:
            await self.__context.wait(delay)
        await self.__intake.trigger_fetch_jobs()

    async def reset_index_fichiers(self):
        self.__logger.info('IntakeHandler trigger fichiers recu')
        await self.__intake.clear_procesing_queue()

        # Supprimer tous les documents indexes
        await self.__solr_dao.reset_index(self.__solr_dao.nom_collection_fichiers, True)

        # Start fetching new jobs
        await self.__intake.trigger_fetch_jobs()

    async def supprimer_tuuids(self, params: dict):
        tuuids = params['tuuids']
        self.__logger.debug("Supprimer tuuids de l'index: %s" % tuuids)
        await self.__solr_dao.supprimer_tuuids(self.__solr_dao.nom_collection_fichiers, tuuids)
        return {'ok': True}
