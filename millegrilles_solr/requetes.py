# Requetes dans l'index
from millegrilles_messages.messages.MessagesModule import MessageWrapper

from millegrilles_solr.EtatRelaiSolr import EtatRelaiSolr
from millegrilles_solr.solrdao import SolrDao


class RequetesHandler:

    def __init__(self, etat_relaisolr: EtatRelaiSolr, solrdao: SolrDao):
        self.__etat_relaisolr = etat_relaisolr
        self.__solrdao = solrdao

    async def traiter_requete(self, message: MessageWrapper):
        raise NotImplementedError()

    async def requete_fichiers(self, user_id: str, query: str):
        nom_collection = self.__solrdao.nom_collection_fichiers
        return await self.__solrdao.requete(nom_collection, user_id, query, qf='name^2 content', start=0, limit=100)
