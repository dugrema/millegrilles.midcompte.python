import aiohttp
import logging
import ssl
import urllib

from os import path
from typing import Optional

from aiohttp.client_exceptions import ClientConnectorError
from cryptography.x509.extensions import ExtensionNotFound

from millegrilles_messages.messages import Constantes
from millegrilles_messages.messages.EnveloppeCertificat import EnveloppeCertificat


class EntretienRabbitMq:

    def __init__(self, etat_midcompte):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__etat_midcompte = etat_midcompte

        self.__passwd_mq: Optional[str] = None
        self.__session: Optional[aiohttp.ClientSession] = None

        ca_path = etat_midcompte.configuration.ca_pem_path
        self.__sslcontext = ssl.create_default_context(cafile=ca_path)

        self.__entretien_initial_complete = False
        self.__url_mq = etat_midcompte.configuration.mq_url

    async def entretien(self):
        self.__logger.debug("entretien debut")

        try:
            if self.__session is None:
                await self.creer_session()

            if self.__session is not None:
                try:
                    path_alarm = path.join(self.__url_mq, 'api/health/checks/alarms')
                    async with self.__session.get(path_alarm, ssl=self.__sslcontext) as reponse:
                        pass
                    self.__logger.debug("Reponse MQ : %s" % reponse)

                    if reponse.status == 200:
                        if self.__entretien_initial_complete is False:
                            await self.entretien_initial()
                        pass  # OK
                    elif reponse.status == 401:
                        self.__logger.warning("Erreur MQ https, tentative de configuration du compte admin")
                        await self.configurer_admin()
                        await self.entretien_initial()
                    elif reponse.status == 503:
                        self.__logger.warning("Erreur MQ https, healthcheck echec")
                except ClientConnectorError:
                    self.__logger.exception("MQ n'est pas accessible")

        except Exception as e:
            self.__logger.exception("Erreur verification RabbitMQ https")

        self.__logger.debug("entretien fin")

    async def creer_session(self):
        if self.__etat_midcompte.configuration.password_mq_path is not None:
            with open(self.__etat_midcompte.configuration.password_mq_path, 'r') as fichier:
                password_mq = fichier.read().strip()
            basic_auth = aiohttp.BasicAuth('admin', password_mq)
            self.__session = aiohttp.ClientSession(auth=basic_auth)

    async def configurer_admin(self):
        with open(self.__etat_midcompte.configuration.password_mq_path, 'r') as fichier:
            password_mq = fichier.read().strip()

        basic_auth = aiohttp.BasicAuth('guest', 'guest')
        async with aiohttp.ClientSession(auth=basic_auth) as session:
            data = {
                'tags': 'administrator',
                'password': password_mq,
            }

            path_admin = path.join(self.__url_mq, 'api/users/admin')
            async with session.put(path_admin, json=data, ssl=self.__sslcontext) as response:
                self.__logger.debug("Reponse creation admin : %s" % response)

    async def entretien_initial(self):
        # S'assurer que guest est supprime
        path_guest = path.join(self.__url_mq, 'api/users/guest')
        async with self.__session.delete(path_guest, ssl=self.__sslcontext) as response:
            self.__logger.debug("Reponse suppression guest : %s" % response)

        idmg = self.__etat_midcompte.idmg
        path_vhosts = path.join(self.__url_mq, 'api/vhosts', idmg)
        async with self.__session.put(path_vhosts, ssl=self.__sslcontext) as response:
            self.__logger.debug("Reponse creation vhost %s : %s" % (idmg, response))
            response.raise_for_status()

        await self.ajouter_exchange(Constantes.SECURITE_SECURE)
        await self.ajouter_exchange(Constantes.SECURITE_PROTEGE)
        await self.ajouter_exchange(Constantes.SECURITE_PRIVE)
        await self.ajouter_exchange(Constantes.SECURITE_PUBLIC)

        self.__entretien_initial_complete = True

    async def creer_usager(self, nom_usager: str):
        nom_usager_parsed = urllib.parse.quote_plus(nom_usager)
        path_creer_usager = path.join(self.__url_mq, 'api/users/%s' % nom_usager_parsed)
        data = {'tags': '', 'password_hash': ''}
        async with self.__session.put(path_creer_usager, json=data, ssl=self.__sslcontext) as reponse:
            status = reponse.status
        return status

    async def creer_permissions_usager(self, nom_usager: str, idmg: str, configure='', write='', read=''):
        vhost_parsed = urllib.parse.quote_plus(idmg)
        nom_usager_parsed = urllib.parse.quote_plus(nom_usager)
        path_creer_permissions = path.join(self.__url_mq, 'api/permissions/%s/%s' % (vhost_parsed, nom_usager_parsed))

        data = {
            'configure': configure or 'amq\\..*',
            'write': write or 'amq\\..*',
            'read': read or 'amq\\..*',
        }

        async with self.__session.put(path_creer_permissions, json=data, ssl=self.__sslcontext) as reponse:
            status = reponse.status

        return status

    async def creer_user_topic(self, nom_usager: str, idmg: str, exchange: str, write='', read=''):
        vhost_parsed = urllib.parse.quote_plus(idmg)
        nom_usager_parsed = urllib.parse.quote_plus(nom_usager)
        path_creer_permissions = path.join(self.__url_mq, 'api/topic-permissions/%s/%s' % (vhost_parsed, nom_usager_parsed))

        data = {
            'exchange': exchange,
            'write': write or 'amq\\..*',
            'read': read or 'amq\\..*',
        }

        async with self.__session.put(path_creer_permissions, json=data, ssl=self.__sslcontext) as reponse:
            status = reponse.status

        return status

    async def ajouter_compte(self, info: dict):
        self.__logger.debug("Ajouter compte dans RabbitMQ: %s" % info)
        dn = info['dn']

        if self.__entretien_initial_complete is False:
            self.__logger.warning("Preparation MQ n'est pas complete, on ne cree pas le compte %s" % dn)
            raise Exception("RabbitMQ n'est pas pret")

        enveloppe: EnveloppeCertificat = info['certificat']
        idmg = enveloppe.idmg

        # subject = enveloppe.subject_rfc4514_string_mq()
        subject = enveloppe.subject_rfc4514_string()
        self.__logger.info("Creation compte MQ pour %s" % subject)

        try:
            # Charger exchanges immediatement - un certificat sans exchanges ne peut pas acceder a mongo/mq
            exchanges = enveloppe.get_exchanges

            responses = list()
            responses.append(await self.creer_usager(subject))

            configure_permissions, read_permissions, write_permissions = get_user_permissions(enveloppe)
            responses.append(await self.creer_permissions_usager(
                subject, idmg, configure=configure_permissions, write=write_permissions, read=read_permissions
            ))

            liste_inclure = {Constantes.SECURITE_PUBLIC}  # PUblic toujours inclus
            if Constantes.SECURITE_PROTEGE in exchanges:
                # pour l'echange protege, on inclus aussi l'echange prive (et public)
                liste_inclure.add(Constantes.SECURITE_PRIVE)
            if Constantes.SECURITE_SECURE in exchanges:
                # pour l'echange secure, on inclus aussi tous les autres echanges
                liste_inclure.add(Constantes.SECURITE_PRIVE)
                liste_inclure.add(Constantes.SECURITE_PROTEGE)
            liste_inclure.update(exchanges)

            liste_exchanges_exclure = [
                Constantes.SECURITE_PUBLIC,
                Constantes.SECURITE_PRIVE,
                Constantes.SECURITE_PROTEGE,
                Constantes.SECURITE_SECURE
            ]

            for exchange in liste_inclure:
                liste_exchanges_exclure.remove(exchange)  # Retire de la liste d'exchanges a exclure
                topic_read_permissions, topic_write_permissions = get_topic_permissions(enveloppe, exchange)
                responses.append(
                    await self.creer_user_topic(
                        subject, idmg, exchange, write=topic_write_permissions, read=topic_read_permissions))

            # Bloquer les exchanges a exclure
            for exchange in liste_exchanges_exclure:
                responses.append(await self.creer_user_topic(subject, idmg, exchange, write='', read=''))

            if any([response is None or response not in [201, 204] for response in responses]):
                raise ValueError("Erreur ajout compte", subject)

        except ExtensionNotFound:
            self.__logger.info("Aucun access a MQ pour certificat %s", subject)
        except Exception as e:
            self.__logger.exception("Erreur traitement ajouter compte %s" % subject)
            raise e
        except:
            self.__logger.exception("Erreur traitement ajouter compte %s" % subject)

    async def ajouter_exchange(self, niveau_securite: str):
        idmg = self.__etat_midcompte.idmg

        params_exchange = {
            "type": "topic",
            "auto_delete": False,
            "durable": True,
            "internal": False
        }

        path_exchange = path.join(self.__url_mq, 'api/exchanges', idmg, niveau_securite)

        async with self.__session.put(path_exchange, ssl=self.__sslcontext, json=params_exchange) as response:
            self.__logger.debug("Reponse creation vhost %s : %s" % (idmg, response))
            response.raise_for_status()

    @property
    def entretien_initial_complete(self):
        return self.__entretien_initial_complete


def get_user_permissions(enveloppe: EnveloppeCertificat):

    read_permissions = ''
    write_permissions = ''
    configure_permissions = ''

    exchanges = '|'.join([e.replace('.', '\\.') for e in enveloppe.get_exchanges])

    # Donner permission de creer/lire/ecrire sur Qs commencant par nom de roles du certificat
    roles = enveloppe.get_roles
    try:
        domaines = enveloppe.get_domaines
        roles.extend(domaines)
    except ExtensionNotFound:
        pass

    role_configs = '|'.join([r + '/.*' for r in roles])
    configure_permissions = '|'.join([role_configs, 'amq.*'])
    read_permissions = '|'.join([role_configs, exchanges, 'amq.*'])
    write_permissions = '|'.join([role_configs, exchanges, 'amq.*'])

    return configure_permissions, read_permissions, write_permissions


def get_topic_permissions(enveloppe: EnveloppeCertificat, exchange: str):
    # Exemple pour media
    # TOPICS (1.public, 2.prive, 3.protege)
    # write, exchanges 2.prive, 3.protege:
    # requete\..*|evenement\.fichiers.*|evenement\.media.*|\..*|commande\..*|transaction\.GrosFichiers\..*|amq\..*
    # read
    # requete\.certificat\..*|evenement\.certificat\..*|requete\.media\..*|evenement\.media\..*|commande\.media\..*|commande\.fichiers\..*|amq.*

    roles = enveloppe.get_roles.copy()
    roles.append('global')
    try:
        domaines = enveloppe.get_domaines
        roles.extend(domaines)
    except ExtensionNotFound:
        pass

    topics_read_roles = [
        'requete\\.%s\\..*',
        'commande\\.%s\\..*',
    ]
    if exchange == Constantes.SECURITE_SECURE:
        topics_read_roles.append('transaction\\.%s\\..*')

    topics_read = [
        'requete\\.certificat\\..*',
        'evenement\\..*',
    ]
    for topic in topics_read_roles:
        for role in roles:
            topics_read.append(topic % role)

    topics_write_roles = [
        'evenement\\.%s\\..*',
    ]
    if exchange == Constantes.SECURITE_SECURE:
        topics_write_roles.append('transaction\\.%s\\..*')
    topics_write = [
        'evenement\\.certificat\\.infoCertificat',
        'requete\\..*',
        'commande\\..*',
    ]
    for topic in topics_write_roles:
        for role in roles:
            topics_write.append(topic % role)

    topics_read = '|'.join(topics_read)
    topics_write = '|'.join(topics_write)

    return topics_read, topics_write

