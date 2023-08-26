import datetime
import logging
import pytz

from cryptography.x509.extensions import ExtensionNotFound
from typing import Optional

from millegrilles_messages.messages import Constantes as ConstantesMilleGrilles
from millegrilles_messages.messages.MessagesModule import MessageWrapper
from millegrilles_messages.MilleGrillesConnecteur import EtatInstance
from millegrilles_messages.messages.MessagesThread import MessagesThread
from millegrilles_messages.messages.MessagesModule import MessageProducerFormatteur
from millegrilles_messages.MilleGrillesConnecteur import CommandHandler as CommandesAbstract

from millegrilles_streaming.Intake import IntakeStreaming
from millegrilles_streaming import Constantes


class InformationFuuid:

    def __init__(self, fuuid):
        self.fuuid = fuuid
        self.taille: Optional[int] = None               # Taille du fichier
        self.position_courante: Optional[int] = None    # Position courante de dechiffrage
        self.path_complet: Optional[str] = None         # Path complet sur disque du fichier dechiffre

    @property
    def est_pret(self):
        return self.path_complet is not None


class CommandHandler(CommandesAbstract):

    def __init__(self, etat_instance: EtatInstance, intake: IntakeStreaming):
        super().__init__()
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__etat_instance = etat_instance
        self.__intake = intake
        self.__messages_thread = None

    def get_routing_keys(self):
        return [
            f'evenement.{Constantes.DOMAINE_GROSFICHIERS}.{Constantes.EVENEMENT_GROSFICHIERS_CHANGEMENT_CONSIGNATION_PRIMAIRE}',
            # 'evenement.GrosFichiers.changementConsignation',  # TODO - evenement n'existe pas encore
        ]

    def configurer_consumers(self, messages_thread: MessagesThread):
        self.__messages_thread = messages_thread

        # res_streaming = RessourcesConsommation(self.callback_reply_q,
        #                                        nom_queue='streaming/volatil', channel_separe=True, est_asyncio=True)
        # res_streaming.ajouter_rk(ConstantesMilleGrilles.SECURITE_PRIVE, 'commande.backup.backupTransactions')
        # messages_thread.ajouter_consumer(res_streaming)

    async def traiter_commande(self, producer: MessageProducerFormatteur, message: MessageWrapper):
        routing_key = message.routing_key
        exchange = message.exchange
        action = routing_key.split('.').pop()
        type_message = routing_key.split('.')[0]
        enveloppe = message.certificat

        try:
            exchanges = enveloppe.get_exchanges
        except ExtensionNotFound:
            exchanges = list()

        try:
            roles = enveloppe.get_roles
        except ExtensionNotFound:
            roles = list()

        try:
            user_id = enveloppe.get_user_id
        except ExtensionNotFound:
            user_id = list()

        try:
            delegation_globale = enveloppe.get_delegation_globale
        except ExtensionNotFound:
            delegation_globale = None

        if type_message == 'commande':
            if ConstantesMilleGrilles.SECURITE_PRIVE in exchanges:
                if action == Constantes.EVENEMENT_GROSFICHIERS_CHANGEMENT_CONSIGNATION_PRIMAIRE:
                    await self.__restauration_handler.restaurer(message)
                elif action == Constantes.COMMANDE_CATALOGUE_TRAITE:
                    await self.__restauration_handler.confirmation_catalogue(message)
                elif action == Constantes.COMMANDE_DOMAINE_TRAITE:
                    await self.__restauration_handler.confirmation_domaine(message)
        elif type_message == 'evenement':
            if ConstantesMilleGrilles.SECURITE_SECURE in exchanges:
                if action == 'cedule':
                    if 'core' in roles:
                        await self.traiter_cedule(producer, message)
            if ConstantesMilleGrilles.SECURITE_PRIVE in exchanges:
                if action == Constantes.EVENEMENT_BACKUP_DOMAINE_MISEAJOUR:
                    await self.__intake.recevoir_evenement(message)

        return False  # Empeche de transmettre un message de reponse

    async def traiter_cedule(self, producer: MessageProducerFormatteur, message: MessageWrapper):
        contenu = message.parsed
        date_cedule = datetime.datetime.fromtimestamp(contenu['estampille'], tz=pytz.UTC)

        now = datetime.datetime.now(tz=pytz.UTC)
        if now - datetime.timedelta(minutes=2) > date_cedule:
            return  # Vieux message de cedule

        weekday = date_cedule.weekday()
        hour = date_cedule.hour
        minute = date_cedule.minute

        if self.__intake.en_cours or self.__etat_instance.backup_inhibe:
            # Ignorer le trigger, backup ou restauration en cours
            return

        if weekday == 0 and hour == 4:
            # Declencher un backup complet
            await self.__intake.trigger_traitement()
        elif minute % 20 == 0:
            await producer.emettre_evenement(
                {'complet': False},
                ConstantesMilleGrilles.DOMAINE_BACKUP,
                Constantes.COMMANDE_DECLENCHER_BACKUP,
                exchanges=ConstantesMilleGrilles.SECURITE_PRIVE
            )

    async def traiter_fuuid(self, fuuid, blocking=True):
        """
        Traite une requete web pour un fuuid.
        :param fuuid:
        :param blocking:
        :return:
        """
        pass
