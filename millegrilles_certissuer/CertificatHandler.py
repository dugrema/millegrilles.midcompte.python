import datetime
import logging
import pathlib
import pytz
from typing import Union

from millegrilles_certissuer.Configuration import ConfigurationWeb
from millegrilles_certissuer.EtatCertissuer import EtatCertissuer
from millegrilles_messages.certificats.CertificatsConfiguration import signer_configuration
from millegrilles_messages.certificats.CertificatsInstance import \
    signer_instance_secure, signer_instance_protege, signer_instance_prive, signer_instance_public
from millegrilles_messages.certificats.CertificatsUsager import signer_usager
from millegrilles_messages.messages import Constantes
from millegrilles_messages.messages.EnveloppeCertificat import EnveloppeCertificat
from millegrilles_messages.messages.CleCertificat import CleCertificat
from millegrilles_messages.certificats.Generes import DUREE_CERT_DEFAUT
from millegrilles_messages.messages.ValidateurCertificats import ValidateurCertificatCache
from millegrilles_messages.messages.ValidateurMessage import ValidateurMessage


class CertificatHandler:

    def __init__(self, configuration: ConfigurationWeb, etat_certissuer: EtatCertissuer):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__configuration = configuration
        self.__etat_certissuer = etat_certissuer
        self.__signing_certs: dict[str, MillegrilleHosting] = dict()

    def list_hosting(self) -> list[str]:
        signing_path = pathlib.Path(self.__etat_certissuer.configuration.signing_path)

        idmg_list = list()
        for item in signing_path.iterdir():
            if not item.is_dir() or item.name == 'logs':
                continue
            idmg_list.append(item.name)

        return idmg_list

    def generer_certificat_instance(self, domain: MillegrilleHosting, old: EnveloppeCertificat, csr: str, securite: str, duree=DUREE_CERT_DEFAUT) -> str:
        signing_key = domain.signing_key

        if securite == Constantes.SECURITE_SECURE:
            enveloppe_certificat = signer_instance_secure(signing_key, csr, duree)
        elif securite == Constantes.SECURITE_PROTEGE:
            enveloppe_certificat = signer_instance_protege(signing_key, csr, duree)
        elif securite == Constantes.SECURITE_PRIVE:
            enveloppe_certificat = signer_instance_prive(signing_key, csr, duree)
        elif securite == Constantes.SECURITE_PUBLIC:
            enveloppe_certificat = signer_instance_public(signing_key, csr, duree)
        else:
            raise Exception('Type securite %s non supporte pour une instance' % securite)

        domain.log_renewal(old, enveloppe_certificat)

        return enveloppe_certificat.chaine_pem()

    def generer_certificat_module(self, domain: MillegrilleHosting, parametres: dict) -> str:
        signing_key = domain.signing_key
        csr = parametres['csr']
        enveloppe = signer_configuration(signing_key, csr, parametres)

        domain.log_new(enveloppe)

        return enveloppe.chaine_pem()

    def generer_certificat_usager(self, domain: MillegrilleHosting, parametres: dict) -> str:
        signing_key = domain.signing_key
        csr = parametres['csr']
        enveloppe = signer_usager(signing_key, csr, parametres)

        # Ajouter le certificat CA
        ca_str = domain.ca_str
        chaine_pem = enveloppe.chaine_pem().copy()
        chaine_pem.append(ca_str)

        domain.log_new(enveloppe)

        return chaine_pem

    async def get_hosting(self, idmg: str):
        try:
            return self.__signing_certs[idmg]
        except KeyError:
            pass  # Try to load the domain

        signing_path = pathlib.Path(self.__etat_certissuer.configuration.signing_path)
        hosting_path = signing_path / idmg
        if not hosting_path.exists():
            raise FileNotFoundError()

        # Load and keep signing_certs
        ca = EnveloppeCertificat.from_file(hosting_path / 'ca.pem')
        # Cert and key are in a combined PEM file
        signing_keycert_path = hosting_path / 'signing.pem'
        signing_key = CleCertificat.from_files(signing_keycert_path, signing_keycert_path)
        pem = signing_key.enveloppe.chaine_pem()
        domain = MillegrilleHosting(ca, signing_key, hosting_path)

        # Ensure the signing certificate chain is valid
        await domain.verify_certificate(pem)

        # Basic cleanup, support 10 cached domains at most
        if len(self.__signing_certs) >= 10:
            self.__signing_certs.clear()

        self.__signing_certs[idmg] = domain
        self.__logger.info(f"Initialized IDMG {idmg}, until {domain.signing_key.enveloppe.not_valid_after}")

        return domain

    async def verify_message(self, message: dict) -> tuple[EnveloppeCertificat, MillegrilleHosting]:
        # Find IDMG of message
        certificate = message['certificat']
        enveloppe = EnveloppeCertificat.from_pem(certificate)
        idmg = enveloppe.idmg

        # Check if this Millegrille is hosted
        domain = await self.get_hosting(idmg)

        # Verify message
        result = await domain.verify_message(message)
        if isinstance(result, EnveloppeCertificat):
            return result, domain

        raise Exception("Invalid message or certificate")


LOGGER = logging.getLogger(__name__)

class MillegrilleHosting:

    def __init__(self, ca: EnveloppeCertificat, signing: CleCertificat, domain_path: pathlib.Path):
        self.__ca = ca
        self.__signing = signing
        self.__idmg = ca.idmg
        self.__log_path = domain_path / 'logs'

        self.__log_path.mkdir(parents=False, exist_ok=True)

        if signing.enveloppe.idmg != self.__idmg:
            raise ValueError('IDMG mismatch between certificates')

        if not signing.enveloppe.date_valide():
            raise ValueError('Signing certificate is expired')

        if not ca.date_valide():
            raise ValueError('CA certificate is expired')

        if not self.__signing.cle_correspondent():
            raise ValueError("Signing key/certificate mismatch")

        self.__validateur_certificats = ValidateurCertificatCache(self.__ca)
        self.__validateur_messages = ValidateurMessage(self.__validateur_certificats)

    @property
    def signing_key(self) -> CleCertificat:
        return self.__signing

    @property
    def ca_str(self) -> str:
        return self.__ca.certificat_pem

    async def verify_message(self, message: dict) -> Union[EnveloppeCertificat, bool]:
        return await self.__validateur_messages.verifier(message)

    async def verify_certificate(self, certificate: Union[bytes, str, list]) -> EnveloppeCertificat:
        return await self.__validateur_certificats.valider(certificate)

    def log_renewal(self, old: EnveloppeCertificat, certificate: EnveloppeCertificat):
        # Log old (revoked)
        fingerprint = old.fingerprint
        try:
            filename = self.__log_path / f'renewed.{certificate.not_valid_after.strftime('%Y%m%d')}.txt'
            with open(filename, 'a') as f:
                f.write(f'{fingerprint}\n')
        except:
            LOGGER.error(f"Error logging renewal (old) certificate {fingerprint}")

        # Log new
        self.log_new(certificate)

    def log_new(self, certificate: EnveloppeCertificat):
        fingerprint = certificate.fingerprint
        try:
            filename = self.__log_path / f'expire.{certificate.not_valid_after.strftime('%Y%m%d')}.txt'
            with open(filename, 'a') as f:
                f.write(f'{fingerprint};{certificate.subject_rfc4514_string_mq()};{datetime.datetime.now(tz=pytz.UTC).isoformat()}\n')
        except:
            LOGGER.error(f"Error logging new certificate {fingerprint}")

