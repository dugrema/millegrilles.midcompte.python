import asyncio
import datetime
import json
import logging
import os
import pathlib
import pytz

from cryptography.x509.extensions import ExtensionNotFound
from typing import Optional

from millegrilles_messages.messages import Constantes as ConstantesMilleGrilles
from millegrilles_messages.messages.MessagesModule import MessageWrapper, RessourcesConsommation
from millegrilles_messages.MilleGrillesConnecteur import EtatInstance
from millegrilles_messages.messages.MessagesThread import MessagesThread
from millegrilles_messages.messages.MessagesModule import MessageProducerFormatteur
from millegrilles_messages.MilleGrillesConnecteur import CommandHandler as CommandesAbstract

from millegrilles_fichiers.Intake import IntakeFichiers
from millegrilles_fichiers import Constantes


class CommandHandler(CommandesAbstract):

    def __init__(self, etat_instance: EtatInstance, intake: IntakeFichiers, consignation):
        super().__init__()
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__etat_instance = etat_instance
        self.__intake = intake
        self.__consignation = consignation
        self.__messages_thread = None

    def get_routing_keys(self):
        return [
            # f'evenement.{Constantes.DOMAINE_GROSFICHIERS}.{Constantes.EVENEMENT_GROSFICHIERS_CHANGEMENT_CONSIGNATION_PRIMAIRE}',
            # 'evenement.GrosFichiers.changementConsignation',  # TODO - evenement n'existe pas encore
        ]

    def configurer_consumers(self, messages_thread: MessagesThread):
        instance_id = self.__etat_instance.clecertificat.enveloppe.subject_common_name
        self.__messages_thread = messages_thread

        # Queue primaire
        # Demarrer sans consumer (actif = False), activer si Topologie confirme qu'on est primaire
        res_primaire = RessourcesConsommation(
            self.callback_reply_q,
            nom_queue=Constantes.QUEUE_PRIMAIRE_NOM, channel_separe=True, est_asyncio=True, actif=False)

        # requete.fichiers.verifierExistance
        res_primaire.ajouter_rk(
            ConstantesMilleGrilles.SECURITE_PRIVE,
            f'requete.{Constantes.DOMAINE_FICHIERS}.{Constantes.REQUETE_FUUID_VERIFIER_EXISTANCE}',)
        # commande.fichiers.declencherSync
        res_primaire.ajouter_rk(
            ConstantesMilleGrilles.SECURITE_PRIVE,
            f'commande.{Constantes.DOMAINE_FICHIERS}.{Constantes.COMMANDE_DECLENCHER_SYNC}',)
        # commande.fichiers.confirmerActiviteFuuids
        res_primaire.ajouter_rk(
            ConstantesMilleGrilles.SECURITE_PRIVE,
            f'commande.{Constantes.DOMAINE_FICHIERS}.{Constantes.COMMANDE_ACTIVITE_FUUIDS}',)

        # Queue d'instance
        res_instance = RessourcesConsommation(
            self.callback_reply_q,
            nom_queue=f"{Constantes.QUEUE_INSTANCE_NOM}/{instance_id}", channel_separe=True, est_asyncio=True, auto_delete=True)

        res_instance.ajouter_rk(
            ConstantesMilleGrilles.SECURITE_PUBLIC,
            f'evenement.{ConstantesMilleGrilles.ROLE_CEDULEUR}.{ConstantesMilleGrilles.EVENEMENT_PING_CEDULE}',
        )

        # Queue anonyme de traitements
        res_evenements = RessourcesConsommation(self.callback_reply_q, channel_separe=True, est_asyncio=True)

        # Sync secondaire
        res_evenements.ajouter_rk(
            ConstantesMilleGrilles.SECURITE_PRIVE,
            f'evenement.{Constantes.DOMAINE_FICHIERS}.{Constantes.EVENEMENT_SYNC_SECONDAIRE}', )
        res_evenements.ajouter_rk(
            ConstantesMilleGrilles.SECURITE_PRIVE,
            f'evenement.{Constantes.DOMAINE_FICHIERS}.{Constantes.EVENEMENT_CONSIGNATION_PRIMAIRE}', )

        # Configuration et topologie
        # evenement.GrosFichiers.changementConsignationPrimaire
        res_evenements.ajouter_rk(
            ConstantesMilleGrilles.SECURITE_PRIVE,
            f'evenement.{Constantes.DOMAINE_CORE_TOPOLOGIE}.{Constantes.EVENEMENT_CHANGEMENT_CONSIGNATION_PRIMAIRE}',)
        # requete.fichiers.INSTANCE_ID.getPublicKeySsh
        res_evenements.ajouter_rk(
            ConstantesMilleGrilles.SECURITE_PRIVE,
            f'requete.{Constantes.DOMAINE_FICHIERS}.{instance_id}.{Constantes.REQUETE_PUBLIC_KEY_SSH}',)
        # requete.fichiers.getPublicKeySsh
        res_evenements.ajouter_rk(
            ConstantesMilleGrilles.SECURITE_PRIVE,
            f'requete.{Constantes.DOMAINE_FICHIERS}.{Constantes.REQUETE_PUBLIC_KEY_SSH}',)
        # commande.fichiers.modifierConfiguration
        res_evenements.ajouter_rk(
            ConstantesMilleGrilles.SECURITE_PRIVE,
            f'commande.{Constantes.DOMAINE_FICHIERS}.{instance_id}.{Constantes.COMMANDE_MODIFIER_CONFIGURATION}',)
        # evenement.CoreTopologie.changementConsignationPrimaire
        res_evenements.ajouter_rk(
            ConstantesMilleGrilles.SECURITE_PRIVE,
            f'evenement.{Constantes.DOMAINE_CORE_TOPOLOGIE}.{instance_id}.{Constantes.EVENEMENT_CHANGEMENT_CONSIGNATION_PRIMAIRE}',)
        # evenement.fichiers.syncPret
        res_evenements.ajouter_rk(
            ConstantesMilleGrilles.SECURITE_PRIVE,
            f'evenement.{Constantes.DOMAINE_FICHIERS}.{instance_id}.{Constantes.EVENEMENT_SYNC_PRET}',)
        # commande.fichiers.reactiverFuuids
        res_primaire.ajouter_rk(
            ConstantesMilleGrilles.SECURITE_PRIVE,
            f'commande.{Constantes.DOMAINE_FICHIERS}.{Constantes.COMMANDE_REACTIVER_FUUIDS}',)

        # Backup
        # commande.fichiers.entretienBackup
        res_evenements.ajouter_rk(
            ConstantesMilleGrilles.SECURITE_PRIVE,
            f'commande.{Constantes.DOMAINE_FICHIERS}.{Constantes.COMMANDE_ENTRETIEN_BACKUP}',)

        # Ajouter threads
        messages_thread.ajouter_consumer(res_primaire)
        messages_thread.ajouter_consumer(res_instance)
        messages_thread.ajouter_consumer(res_evenements)

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
                if action == Constantes.COMMANDE_MODIFIER_CONFIGURATION:
                    await self.__consignation.modifier_topologie(message.parsed)
                elif action == Constantes.EVENEMENT_CHANGEMENT_CONSIGNATION_PRIMAIRE:
                    await self.__consignation.charger_topologie()
                elif action == Constantes.COMMANDE_ENTRETIEN_BACKUP:
                    await self.__consignation.rotation_backup(message.parsed)
                elif action == Constantes.COMMANDE_ACTIVITE_FUUIDS:
                    await self.__consignation.conserver_activite_fuuids(message.parsed)
                elif action == Constantes.COMMANDE_REACTIVER_FUUIDS:
                    return await self.__consignation.reactiver_fuuids(message.parsed)
                else:
                    self.__logger.warning(
                        "Commande non supportee (action %s) - SKIP" % action)
            elif ConstantesMilleGrilles.DELEGATION_GLOBALE_PROPRIETAIRE == delegation_globale:
                if action == Constantes.COMMANDE_REACTIVER_FUUIDS:
                    return await self.__consignation.reactiver_fuuids(message.parsed)
                elif action == Constantes.COMMANDE_DECLENCHER_SYNC:
                    return await self.__consignation.declencher_sync_primaire(message.parsed)
                else:
                    self.__logger.warning(
                        "Commande non supportee pour delegation globale (action %s) - SKIP" % action)
            elif user_id is not None:
                if action == Constantes.COMMANDE_REACTIVER_FUUIDS:
                    return await self.__consignation.reactiver_fuuids(message.parsed)
                else:
                    self.__logger.warning(
                        "Commande non supportee pour usager (action %s) - SKIP" % action)
            else:
                self.__logger.warning("Commande non supportee (exchange %s, action %s) - SKIP" % (exchanges, action))
        elif type_message == 'requete':
            if action == Constantes.REQUETE_PUBLIC_KEY_SSH:
                return await self.requete_cles_ssh()
            elif action == Constantes.REQUETE_FUUID_VERIFIER_EXISTANCE:
                return await self.verifier_existance(message.parsed)
            else:
                self.__logger.warning(
                    "Requete non supportee (action %s) - SKIP" % action)
        elif type_message == 'evenement':
            if action == ConstantesMilleGrilles.EVENEMENT_PING_CEDULE and exchange == ConstantesMilleGrilles.SECURITE_PUBLIC:
                await self.traiter_cedule(producer, message)
            if ConstantesMilleGrilles.SECURITE_PRIVE in exchanges:
                if action == Constantes.EVENEMENT_CHANGEMENT_CONSIGNATION_PRIMAIRE:
                    await self.__consignation.charger_topologie()
                elif action == Constantes.EVENEMENT_SYNC_PRET:
                    raise NotImplementedError('todo')
                elif action == Constantes.EVENEMENT_SYNC_SECONDAIRE:
                    return await self.__consignation.declencher_sync_secondaire(message.parsed)
                elif action == Constantes.EVENEMENT_CONSIGNATION_PRIMAIRE:
                    await self.__consignation.ajouter_fuuid_primaire(message.parsed)
                else:
                    self.__logger.warning(
                        "Evenement non supporte (action %s) - SKIP" % action)
            else:
                self.__logger.warning("Commande non supportee (exchange %s, action %s) - SKIP" % (exchanges, action))
        else:
            self.__logger.warning("Type message non supporte (type %s, exchange %s, action %s) - SKIP" % (type_message, exchanges, action))

        return False  # Empeche de transmettre un message de reponse

    async def traiter_cedule(self, producer: MessageProducerFormatteur, message: MessageWrapper):
        if ConstantesMilleGrilles.SECURITE_PUBLIC not in message.certificat.get_exchanges:
            self.__logger.warning("traiter_cedule Message sans l'exchange 1.public - SKIP")
        elif ConstantesMilleGrilles.ROLE_CEDULEUR not in message.certificat.get_roles:
            self.__logger.warning("traiter_cedule Message sans le role ceduleur - SKIP")

        contenu = message.parsed
        date_cedule = datetime.datetime.fromtimestamp(contenu['estampille'], tz=pytz.UTC)

        now = datetime.datetime.now(tz=pytz.UTC)
        if now - datetime.timedelta(seconds=80) > date_cedule:
            return  # Vieux message de cedule

        await self.__consignation.traiter_cedule(producer, message)

    async def requete_cles_ssh(self):
        cles_ssh = self.__etat_instance.get_public_key_ssh()

        reponse = {
            'clePubliqueEd25519': cles_ssh['ed25519'],
            'clePubliqueRsa': cles_ssh['rsa']
        }

        return reponse

    async def verifier_existance(self, commande: dict):
        fuuids = commande['fuuids']
        visiter = commande.get('visiter')
        if visiter is True:
            self.__logger.warning('verifier_existance Flag visiter==true, non implemente (IGNORE)')

        info_fuuids = dict()
        reponse = {'fuuids': info_fuuids}

        for fuuid in fuuids:
            info_fuuids[fuuid] = False

            info = self.__consignation.get_info_fichier(fuuid)
            if info is not None:
                info_fuuids[fuuid] = info['etat_fichier'] != Constantes.DATABASE_ETAT_MANQUANT

        return reponse
