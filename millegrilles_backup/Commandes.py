import logging

from cryptography.x509.extensions import ExtensionNotFound

from millegrilles_messages.messages import Constantes as ConstantesMilleGrilles
from millegrilles_messages.messages.MessagesModule import MessageWrapper
from millegrilles_messages.MilleGrillesConnecteur import EtatInstance
from millegrilles_messages.messages.MessagesThread import MessagesThread
from millegrilles_messages.messages.MessagesModule import RessourcesConsommation, MessageProducerFormatteur
from millegrilles_messages.MilleGrillesConnecteur import CommandHandler as CommandesAbstract

from millegrilles_backup.Intake import IntakeBackup


class CommandHandler(CommandesAbstract):

    def __init__(self, etat_instance: EtatInstance, intake_backups: IntakeBackup):
        super().__init__()
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__etat_instance = etat_instance
        self.__intake_backups = intake_backups
        self.__messages_thread = None

    def get_routing_keys(self):
        return [
            'evenement.backup.changementPrimaire',
        ]

    def configurer_consumers(self, messages_thread: MessagesThread):
        self.__messages_thread = messages_thread

        res_primaire = RessourcesConsommation(self.callback_reply_q,
                                              nom_queue='backup/primaire', channel_separe=True, est_asyncio=True)
        res_primaire.ajouter_rk(ConstantesMilleGrilles.SECURITE_PRIVE, 'commande.backup.demarrerBackupTransactions')
        res_primaire.ajouter_rk(ConstantesMilleGrilles.SECURITE_PRIVE, 'commande.backup.getClesBackupTransactions')
        res_primaire.ajouter_rk(ConstantesMilleGrilles.SECURITE_PRIVE, 'requete.backup.getBackupTransaction')
        messages_thread.ajouter_consumer(res_primaire)

        res_backup = RessourcesConsommation(self.callback_reply_q,
                                            nom_queue='backup/transactions', channel_separe=True, est_asyncio=True)
        res_backup.ajouter_rk(ConstantesMilleGrilles.SECURITE_PRIVE, 'commande.backup.backupTransactions')
        messages_thread.ajouter_consumer(res_backup)

    async def traiter_commande(self, producer: MessageProducerFormatteur, message: MessageWrapper):
        reponse = None

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
            user_id = enveloppe.get_user_id
        except ExtensionNotFound:
            user_id = list()

        try:
            delegation_globale = enveloppe.get_delegation_globale
        except ExtensionNotFound:
            delegation_globale = None

        if exchange == ConstantesMilleGrilles.SECURITE_PRIVE and ConstantesMilleGrilles.SECURITE_PRIVE in exchanges:
            if type_message == 'evenement':
                pass
                # if action in [ConstantesMedia.EVENEMENT_JOB_IMAGE_DISPONIBLE, ConstantesMedia.EVENEMENT_JOB_VIDEO_DISPONIBLE]:
                #     await self._intake_images.trigger_traitement()
                #     if self._intake_videos is not None:
                #         await self._intake_videos.trigger_traitement()
                #     return
                # elif action == ConstantesMedia.EVENEMENT_ANNULER_JOB_VIDEO:
                #     await self._intake_videos.annuler_job(message.parsed, False)

        return reponse
