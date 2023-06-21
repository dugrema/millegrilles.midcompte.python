# Requetes dans l'index
from cryptography.x509.extensions import ExtensionNotFound

from millegrilles_messages.messages.MessagesModule import MessageWrapper

from millegrilles_solr.EtatRelaiSolr import EtatRelaiSolr
from millegrilles_solr.solrdao import SolrDao
from millegrilles_solr import Constantes as ConstantesRelaiSolr


class RequetesHandler:

    def __init__(self, etat_relaisolr: EtatRelaiSolr, solrdao: SolrDao):
        self.__etat_relaisolr = etat_relaisolr
        self.__solrdao = solrdao

    async def traiter_requete(self, message: MessageWrapper):

        routing_key = message.routing_key
        action = routing_key.split('.').pop()
        enveloppe = message.certificat

        try:
            roles = enveloppe.get_roles
        except ExtensionNotFound:
            roles = list()

        try:
            user_id = enveloppe.get_user_id
        except ExtensionNotFound:
            user_id = None

        if action == ConstantesRelaiSolr.REQUETE_FICHIERS:
            query = message.parsed['query']
            start = message.parsed.get('start') or 0
            limit = message.parsed.get('limit') or 200
            reponse = await self.requete_fichiers(user_id, query, start=start, limit=limit)
            return {'ok': True, 'resultat': reponse['response']}
        else:
            raise Exception('action requete non supportee : %s' % action)

        # if reponse is not None:
        #     reponse = await self.chiffrer_reponse(enveloppe, reponse)

        return None

    async def requete_fichiers(self, user_id: str, query: str, start=0, limit=200):
        nom_collection = self.__solrdao.nom_collection_fichiers
        return await self.__solrdao.requete(
            nom_collection, user_id, query, qf='name^2 content id tuuid', start=start, limit=limit)

    async def chiffrer_reponse(self, enveloppe, reponse: dict):
        raise NotImplementedError('todo')
