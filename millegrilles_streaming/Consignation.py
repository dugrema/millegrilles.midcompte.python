from urllib.parse import urljoin, urlparse

import aiohttp
import asyncio
import logging
import ssl

from ssl import SSLContext
from typing import Optional

from urllib3.util import parse_url

from millegrilles_media.EtatMedia import EtatMedia
from millegrilles_messages.messages import Constantes
from millegrilles_messages.chiffrage.DechiffrageUtils import get_decipher_cle_secrete
from millegrilles_streaming.EtatStreaming import EtatStreaming


class ConsignationHandler:
    """
    Download et dechiffre les fichiers de media a partir d'un serveur de consignation
    """

    def __init__(self, stop_event: asyncio.Event, etat_instance: EtatStreaming):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__stop_event = stop_event
        self.__etat_instance = etat_instance

        # self.__url_consignation: Optional[str] = None
        self.__filehost: Optional[dict] = None
        self.__filehost_url: Optional[str] = None
        self.__tls_method: Optional[str] = None
        self.__session_http_requests: Optional[aiohttp.ClientSession] = None

    async def run(self):
        self.__logger.info("Demarrage run")
        await asyncio.gather(self.entretien())
        self.__logger.info("Fin run")

    async def entretien(self):
        # Attendre 5 secondes avant le premier entretien
        #await asyncio.wait([stop_event_wait], timeout=5)

        while self.__stop_event.is_set() is False:
            # await self.charger_filehost()
            await self.ouvrir_sessions()
            try:
                await asyncio.wait_for(self.__stop_event.wait(), timeout=900)
            except asyncio.TimeoutError:
                pass

    async def ouvrir_sessions(self):
        await self.charger_filehost()
        # timeout = aiohttp.ClientTimeout(connect=5, total=300)
        # self.__session_http_download = aiohttp.ClientSession(timeout=timeout)
        # await self.filehost_authenticate(self.__session_http_download)

        try:
            if self.__session_http_requests and self.__session_http_requests.closed is False:
                await self.__session_http_requests.close()
        except:
            self.__logger.exception("Erreur rotation session http (fermeture precedente)")

        timeout_requests = aiohttp.ClientTimeout(connect=5, total=15)
        self.__session_http_requests = aiohttp.ClientSession(timeout=timeout_requests)
        await self.filehost_authenticate(self.__session_http_requests)

    async def charger_filehost(self):
        producer = self.__etat_instance.producer
        if producer is None:
            await asyncio.sleep(5)  # Attendre connexion MQ
            producer = self.__etat_instance.producer
            if producer is None:
                raise Exception('producer pas pret')
        await asyncio.wait_for(producer.producer_pret().wait(), 30)

        # reponse = await producer.executer_requete(
        #     dict(), 'CoreTopologie', 'getConsignationFichiers', exchange="2.prive")
        reponse = await producer.executer_requete(
            dict(), 'CoreTopologie', 'getFilehostForInstance', exchange="1.public")

        try:
            # consignation_url = reponse.parsed['consignation_url']
            # self.__url_consignation = consignation_url
            # return consignation_url
            filehost_response = reponse.parsed
            filehost = filehost_response['filehost']
            self.preparer_filehost_url_context(filehost)
            self.__filehost = filehost
        except Exception as e:
            self.__logger.exception("Erreur chargement URL consignation")

    def preparer_filehost_url_context(self, filehost: dict):
        if filehost.get('instance_id') == self.__etat_instance.instance_id and filehost.get('url_internal') is not None:
            self.__filehost_url = urljoin(filehost['url_internal'], '/filehost')
            # Connecter avec certificat interne
        elif filehost.get('url_external') is not None and filehost.get('tls_external') is not None:
            self.__filehost_url = urljoin(filehost['url_external'], '/filehost')
            self.__tls_method = filehost['tls_external']
        else:
            raise ValueError('No acceptable URL')

        # Apply configuration
        self.__configure_ssl()

    def __configure_ssl(self):
        if self.__tls_method is None:
            return  # Nothing to do

        if self.__tls_method == 'millegrille':
            # Internal mode
            ssl_context = SSLContext()
            ssl_context.load_cert_chain(self.__etat_instance.configuration.cert_pem_path, self.__etat_instance.configuration.key_pem_path)
        elif self.__tls_method == 'external':
            # Internet mode
            ssl_context = ssl.create_default_context()
        elif self.__tls_method == 'nocheck':
            # No certificate check (insecure) - also need to use verify=False on requests
            ssl_context = ssl.create_default_context()
        else:
            raise ValueError('Unknown TLS check method: %s' % self.__tls_method)

        self.__ssl_context = ssl_context

    async def download_fichier(self, fuuid, cle_dechiffree, params_dechiffrage, path_destination):
        # await self.ouvrir_sessions()  # S'assurer d'avoir une session ouverte
        url_fuuid = self.get_url_fuuid(fuuid)

        # clecert = self.__etat_instance.clecertificat
        # decipher = get_decipher(clecert, cle_dechiffree, params_dechiffrage)
        decipher = get_decipher_cle_secrete(cle_dechiffree, params_dechiffrage)

        timeout = aiohttp.ClientTimeout(connect=30, total=900)
        with path_destination.open(mode='wb') as output_file:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                await self.filehost_authenticate(session)

                async with session.get(url_fuuid, ssl=self.__etat_instance.ssl_context) as resp:
                    resp.raise_for_status()

                    async for chunk in resp.content.iter_chunked(64*1024):
                        await asyncio.to_thread(output_file.write, decipher.update(chunk))

            output_file.write(decipher.finalize())

    async def verifier_existance(self, fuuid: str) -> dict:
        """
        Requete HEAD pour verifier que le fichier existe sur la consignation locale.
        :param fuuid:
        :return:
        """
        # await self.ouvrir_sessions()  # S'assurer d'avoir une session ouverte
        url_fuuid = self.get_url_fuuid(fuuid)
        reponse = await self.__session_http_requests.head(url_fuuid, ssl=self.__ssl_context)
        if reponse.status == 401:
            # Cookie expired, try again
            await self.filehost_authenticate(self.__session_http_requests)
            reponse = await self.__session_http_requests.head(url_fuuid, ssl=self.__ssl_context)
        reponse.raise_for_status()
        return {'taille': reponse.headers.get('Content-Length'), 'status': reponse.status}

    def get_url_fuuid(self, fuuid) -> str:
        if self.__filehost_url is None:
            raise Exception('Filehost Url not set')
        filehost_url = urlparse(self.__filehost_url)
        filehost_path = filehost_url.path + f'/files/{fuuid}'
        filehost_path = filehost_path.replace('//', '/')
        url_fuuid = urljoin(self.__filehost_url, filehost_path)
        return url_fuuid

    async def filehost_authenticate(self, session):
        filehost_url = urlparse(self.__filehost_url)
        filehost_path = filehost_url.path + '/authenticate'
        filehost_path = filehost_path.replace('//', '/')
        url_authenticate = urljoin(filehost_url.geturl(), filehost_path)
        authentication_message, message_id = self.__etat_instance.formatteur_message.signer_message(
            Constantes.KIND_COMMANDE, dict(), domaine='filehost', action='authenticate')
        authentication_message['millegrille'] = self.__etat_instance.formatteur_message.enveloppe_ca.certificat_pem
        async with session.post(url_authenticate, json=authentication_message, ssl_context=self.__ssl_context) as resp:
            resp.raise_for_status()
