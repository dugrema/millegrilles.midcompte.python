from typing import Optional

from millegrilles_solr.solrdao import SolrDao


class RequetesHandler:

    def __init__(self, solr_dao: SolrDao):
        self.__solr_dao = solr_dao

    async def traiter_requete(self, user_id: str, params: dict):
        query = params['query']
        start = params.get('start') or 0
        limit = params.get('limit') or 200
        try:
            cuuids = params['cuuids_partages']
            if len(cuuids) == 0:
                cuuids = None
        except (TypeError, KeyError):
            cuuids = None

        reponse = await self.requete_fichiers(user_id, query, cuuids=cuuids, start=start, limit=limit)

        return {'ok': True, 'resultat': reponse['response']}

    async def requete_fichiers(self, user_id: str, query: str, cuuids: Optional[list] = None, start=0, limit=200):
        nom_collection = self.__solr_dao.nom_collection_fichiers

        # cuuids = ['f5d212201429f0bfcc9f7f35a111bc79ef0068bf9662d270aae94da93db15645', '80e94d2ec4fe060a67335eb69fe48c2853de1e3693b558a95f9bd7c96c2dd983', '9ff552db7ae81e7ff4006b178ecac7be7f6a706bb3b76b1651cd656ded5eafb7']
        # user_id = 'pas_moi'

        return await self.__solr_dao.requete(
            nom_collection, user_id, query,
            qf='name^2 content id fuuid hachage_original',
            cuuids=cuuids,
            start=start,
            limit=limit
        )
