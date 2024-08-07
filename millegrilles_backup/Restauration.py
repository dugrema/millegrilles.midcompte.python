import asyncio
import gzip
import json
import logging
import lzma
import os

from typing import Optional

from millegrilles_messages.messages import Constantes as ConstantesMilleGrilles
from millegrilles_messages.messages.MessagesModule import MessageWrapper
from millegrilles_messages.messages.MessagesModule import MessageProducerFormatteur
from millegrilles_messages.MilleGrillesConnecteur import EtatInstance


class HandlerRestauration:

    def __init__(self, stop_event: asyncio.Event, etat_instance: EtatInstance):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__stop_event = stop_event
        self.__etat_instance = etat_instance

        self.__queue_triggers: Optional[asyncio.Queue] = None
        self.__domaines_restauration: Optional[list] = None
        self.__event_attendre_confirmation: Optional[asyncio.Event] = None

    async def run(self):
        self.__queue_triggers = asyncio.Queue(maxsize=2)

        coro_stop_event = asyncio.create_task(self.__stop_event.wait())
        while self.__stop_event.is_set() is False:
            done, pending = await asyncio.wait([coro_stop_event, self.__queue_triggers.get()], return_when=asyncio.FIRST_COMPLETED)
            message = done.pop().result()
            if isinstance(message, MessageWrapper):
                self.__logger.info("Recu trigger restauration")
                try:
                    await self.__restaurer(message)
                except:
                    self.__logger.exception("Erreur traitement restauration")

    async def restaurer(self, message: MessageWrapper):
        await self.__queue_triggers.put(message)

    async def __restaurer(self, message: MessageWrapper):
        # Arreter le declenchement des triggers de backup
        self.__etat_instance.backup_inhibe = True

        domaines = message.parsed.get('domaines')

        try:
            self.__domaines_restauration = self.lister_domaines(domaines)

            if self.__domaines_restauration is not None and len(self.__domaines_restauration) > 0:
                # On a une liste de domaines, backup est possible
                # Repondre pour que le client commence a ecouter les fichiers
                await self.repondre_demarrage(message, ok=True, domaines=self.__domaines_restauration)
            else:
                await self.repondre_demarrage(message, ok=False)
                return  # Abort

            self.__event_attendre_confirmation = asyncio.Event()
            for domaine in self.__domaines_restauration:
                await self.uploader_domaine(domaine, message)
            self.__event_attendre_confirmation = None

            # Emettre reponse domaine complete
            producer = self.__etat_instance.producer
            await producer.producer_pret().wait()
            reponse = {'ok': True, 'restauration_completee': True}
            await producer.repondre(reponse, reply_to=message.reply_to, correlation_id='restaurationCompletee')

        finally:
            # Reactiver les triggers de backup
            self.__etat_instance.backup_inhibe = False

    async def repondre_demarrage(self, message: MessageWrapper, ok=False, domaines=None):
        producer = self.__etat_instance.producer
        await producer.producer_pret().wait()

        if domaines is not None:
            domaines_filtres = list()
            for domaine in domaines:
                domaines_filtres.append({
                    'domaine': domaine['domaine'],
                    'file_count': domaine['count'],
                    'regenerer_immediatement': domaine.get('regenerer_immediatement') or False,
                })
            domaines = domaines_filtres

        reponse = {
            'ok': ok,
            'domaines': domaines
        }

        await producer.repondre(reponse, reply_to=message.reply_to, correlation_id=message.correlation_id)

    def lister_domaines(self, filtre: Optional[list]) -> list[dict]:
        """
        Faire la liste des domaines disponibles dans le backup
        :return:
        """
        dir_backup = self.__etat_instance.configuration.dir_backup
        dir_transactions = os.path.join(dir_backup, 'transactions')

        domaines = list()
        for rep in os.listdir(dir_transactions):
            if filtre is None or rep in filtre:
                path_domaine = os.path.join(dir_transactions, rep)
                nombre_fichiers = len(os.listdir(path_domaine))
                if os.path.isdir(path_domaine):
                    if rep == 'CorePki':
                        # Inserer en premier, doit etre executer avant tous les autres
                        domaines.insert(0, {'domaine': rep, 'fullpath': path_domaine, 'count': nombre_fichiers, 'regenerer_immediatement': True})
                    else:
                        domaines.append({'domaine': rep, 'fullpath': path_domaine, 'count': nombre_fichiers})

        return domaines

    async def uploader_domaine(self, domaine: dict, message: MessageWrapper):
        producer = self.__etat_instance.producer
        await producer.producer_pret().wait()

        nom_domaine = domaine['domaine']
        rep_path = domaine['fullpath']
        task_stop_event = asyncio.Task(self.__stop_event.wait())

        for catalogue in self.generateur_fichiers(rep_path):
            # self.__logger.debug("Traiter catalogue : %s" % catalogue)
            self.__event_attendre_confirmation.clear()

            # Emettre le catalogue dans un wrapper pour encapsuler un message avec certificat potentiellement expire
            wrapper_catalogue = {'catalogue': catalogue}
            await producer.repondre(wrapper_catalogue,
                                    reply_to=message.reply_to, correlation_id='catalogueTransactions')

            # Attendre confirmation que le catalogue a ete traite
            await asyncio.wait([task_stop_event, self.__event_attendre_confirmation.wait()],
                               return_when=asyncio.FIRST_COMPLETED, timeout=300)

            if self.__stop_event.is_set():
                raise Exception("uploader_domaine stopped")
            elif self.__event_attendre_confirmation.is_set():
                # Confirmation recue, passer au prochain catalogue
                pass
            else:
                raise Exception('uploader_domaine Timeout attente confirmation traitement catalogue transactions domaine %s' % nom_domaine)

        # Emettre reponse domaine complete
        # self.__event_attendre_confirmation.clear()
        reponse = {'ok': True, 'domaine': nom_domaine, 'domaine_complete': True}
        await producer.repondre(reponse, reply_to=message.reply_to, correlation_id='domaineComplete')
        # Attendre confirmation que le catalogue a ete traite
        # await asyncio.wait([task_stop_event, self.__event_attendre_confirmation.wait()],
        #                    return_when=asyncio.FIRST_COMPLETED, timeout=5)

        # if self.__event_attendre_confirmation.is_set() is False:
        #     self.__logger.warning("Erreur attente confirmation domaine %s complete, on passe au prochain" % nom_domaine)
        # else:
        #     task_stop_event.cancel()  # Annuler task attente stop_event

        task_stop_event.cancel()  # Annuler task attente stop_event

    def generateur_fichiers(self, path: str):
        for nom_fichier in os.listdir(path):
            path_complet = os.path.join(path, nom_fichier)
            self.__logger.info("Ouvrir fichier %s" % path_complet)

            if nom_fichier.endswith('.json.xz'):
                with lzma.open(path_complet) as fichier:
                    contenu = json.load(fichier)
            elif nom_fichier.endswith('.json.gz'):
                with gzip.open(path_complet) as fichier:
                    contenu = json.load(fichier)
            elif nom_fichier.endswith('.json'):
                with open(path_complet) as fichier:
                    contenu = json.load(fichier)
            else:
                raise Exception("Type fichier non supporte : %s" % path_complet)

            yield contenu

        return None

    async def transmettre_catalogue(self, producer: MessageProducerFormatteur, message: MessageWrapper, domaine: dict, catalogue: dict):

        pass

    async def confirmation_catalogue(self, message: MessageWrapper):
        self.__event_attendre_confirmation.set()

    async def confirmation_domaine(self, message: MessageWrapper):
        self.__event_attendre_confirmation.set()

    async def emettre_maj_restauration(self):
        producer = self.__etat_instance.producer
        await producer.producer_pret().wait()

        evenement = {
            'domaines': self.__domaines_restauration
        }
        await producer.emettre_evenement(
            evenement,
            ConstantesMilleGrilles.DOMAINE_BACKUP,
            "restaurationMaj",
            exchanges=ConstantesMilleGrilles.SECURITE_PRIVE
        )
