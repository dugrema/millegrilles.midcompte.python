# Requetes dans l'index
from typing import Optional

from cryptography.x509.extensions import ExtensionNotFound

from millegrilles_messages.messages import Constantes as ContantesMillegrilles
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
        exchange = message.exchange
        action = routing_key.split('.').pop()
        enveloppe = message.certificat

        try:
            user_id = enveloppe.get_user_id
        except ExtensionNotFound:
            domaines = enveloppe.get_domaines
            if ContantesMillegrilles.SECURITE_PROTEGE in enveloppe.get_exchanges and exchange == ContantesMillegrilles.SECURITE_PROTEGE and \
                    'GrosFichiers' in domaines:
                user_id = message.parsed['user_id']
            else:
                user_id = None

        if user_id is None:
            return {'ok': False, 'err': 'user_id manquant'}

        if action == ConstantesRelaiSolr.REQUETE_FICHIERS:
            query = message.parsed['query']
            start = message.parsed.get('start') or 0
            limit = message.parsed.get('limit') or 200
            try:
                cuuids = message.parsed['cuuids_partages']
                if len(cuuids) == 0:
                    cuuids = None
            except (TypeError, KeyError):
                cuuids = None

            reponse = await self.requete_fichiers(user_id, query, cuuids=cuuids, start=start, limit=limit)

            return {'ok': True, 'resultat': reponse['response']}
        else:
            raise Exception('action requete non supportee : %s' % action)

    async def requete_fichiers(self, user_id: str, query: str, cuuids: Optional[list] = None, start=0, limit=200):
        nom_collection = self.__solrdao.nom_collection_fichiers

        # cuuids = ['f5d212201429f0bfcc9f7f35a111bc79ef0068bf9662d270aae94da93db15645', '80e94d2ec4fe060a67335eb69fe48c2853de1e3693b558a95f9bd7c96c2dd983', '9ff552db7ae81e7ff4006b178ecac7be7f6a706bb3b76b1651cd656ded5eafb7']
        # user_id = 'pas_moi'

        return await self.__solrdao.requete(
            nom_collection, user_id, query,
            qf='name^2 content id fuuid hachage_original',
            cuuids=cuuids,
            start=start,
            limit=limit
        )

    async def chiffrer_reponse(self, enveloppe, reponse: dict):
        raise NotImplementedError('todo')
