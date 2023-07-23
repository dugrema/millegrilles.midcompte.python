import datetime
import logging

import pytz
from cryptography.x509.extensions import ExtensionNotFound

from millegrilles_messages.messages import Constantes as ConstantesMilleGrilles
from millegrilles_messages.messages.MessagesModule import MessageWrapper
from millegrilles_messages.MilleGrillesConnecteur import EtatInstance
from millegrilles_messages.messages.MessagesThread import MessagesThread
from millegrilles_messages.messages.MessagesModule import RessourcesConsommation, MessageProducerFormatteur
from millegrilles_messages.MilleGrillesConnecteur import CommandHandler as CommandesAbstract

from millegrilles_backup.Intake import IntakeBackup
from millegrilles_backup import Constantes
from millegrilles_backup.Restauration import HandlerRestauration


class CommandHandler(CommandesAbstract):

    def __init__(self, etat_instance: EtatInstance, intake_backups: IntakeBackup, restauration_handler: HandlerRestauration):
        super().__init__()
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__etat_instance = etat_instance
        self.__intake_backups = intake_backups
        self.__restauration_handler = restauration_handler
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
        res_primaire.ajouter_rk(ConstantesMilleGrilles.SECURITE_PRIVE, 'commande.backup.restaurerTransactions')
        res_primaire.ajouter_rk(ConstantesMilleGrilles.SECURITE_PRIVE, 'commande.backup.catalogueTraite')
        res_primaire.ajouter_rk(ConstantesMilleGrilles.SECURITE_PRIVE, 'evenement.*.backupMaj')
        res_primaire.ajouter_rk(ConstantesMilleGrilles.SECURITE_PRIVE, 'evenement.global.cedule')
        messages_thread.ajouter_consumer(res_primaire)

        res_backup = RessourcesConsommation(self.callback_reply_q,
                                            nom_queue='backup/transactions', channel_separe=True, est_asyncio=True)
        res_backup.ajouter_rk(ConstantesMilleGrilles.SECURITE_PRIVE, 'commande.backup.backupTransactions')
        messages_thread.ajouter_consumer(res_backup)

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
            if delegation_globale == ConstantesMilleGrilles.DELEGATION_GLOBALE_PROPRIETAIRE:
                if action == Constantes.COMMANDE_DEMARRER_BACKUP:
                    self.__logger.info("Trigger nouveau backup")
                    deja_en_cours = await self.__intake_backups.trigger_traitement()
                    if deja_en_cours:
                        return {'ok': True, 'message': 'Backup deja en cours', 'code': 2}
                    else:
                        return {'ok': True, 'message': 'Backup demarre', 'code': 1}
            if ConstantesMilleGrilles.SECURITE_SECURE in exchanges:
                if action == Constantes.COMMANDE_BACKUP_TRANSACTIONS:
                    return await self.__intake_backups.recevoir_fichier_transactions(message)
            if ConstantesMilleGrilles.SECURITE_PROTEGE in exchanges:
                if action == Constantes.COMMANDE_RESTAURER_TRANSACTIONS:
                    await self.__restauration_handler.restaurer(message)
                elif action == Constantes.COMMANDE_CATALOGUE_TRAITE:
                    await self.__restauration_handler.confirmation_catalogue(message)
        elif type_message == 'evenement':
            if ConstantesMilleGrilles.SECURITE_SECURE in exchanges:
                if action == 'cedule':
                    if 'core' in roles:
                        await self.traiter_cedule(producer, message)
            if ConstantesMilleGrilles.SECURITE_PRIVE in exchanges:
                if action == Constantes.EVENEMENT_BACKUP_DOMAINE_MISEAJOUR:
                    await self.__intake_backups.recevoir_evenement(message)

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

        if self.__intake_backups.en_cours or self.__etat_instance.backup_inhibe:
            # Ignorer le trigger, backup ou restauration en cours
            return

        if weekday == 0 and hour == 4:
            # Declencher un backup complet
            await self.__intake_backups.trigger_traitement()
        elif minute % 20 == 0:
            await producer.emettre_evenement(
                {'complet': False},
                ConstantesMilleGrilles.DOMAINE_BACKUP,
                Constantes.COMMANDE_DECLENCHER_BACKUP,
                exchanges=ConstantesMilleGrilles.SECURITE_PRIVE
            )
