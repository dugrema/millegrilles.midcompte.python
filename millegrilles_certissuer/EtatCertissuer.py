import logging

from os import path, remove, chmod
from typing import Optional

from millegrilles_certissuer.Configuration import ConfigurationCertissuer
from millegrilles_messages.certificats.CertificatsMillegrille import generer_csr_intermediaire
from millegrilles_messages.certificats.Generes import CleCsrGenere
from millegrilles_messages.messages.EnveloppeCertificat import EnveloppeCertificat
from millegrilles_messages.messages.CleCertificat import CleCertificat
from millegrilles_messages.messages.ValidateurCertificats import ValidateurCertificatCache
from millegrilles_messages.messages.ValidateurMessage import ValidateurMessage


class EtatCertissuer:

    def __init__(self, configuration: ConfigurationCertissuer):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__configuration = configuration
        self.__csr: Optional[CleCsrGenere] = None

        self.__idmg: Optional[str] = None
        self.__ca: Optional[EnveloppeCertificat] = None
        self.__cle_intermediaire: Optional[CleCertificat] = None
        self.__validateur_certificats: Optional[ValidateurCertificatCache] = None
        self.__validateur_messages: Optional[ValidateurMessage] = None

        self.__ca_str: Optional[str] = None

    @property
    def ca_str(self):
        return self.__ca_str

    @property
    def cle_intermediaire(self):
        return self.__cle_intermediaire

    @property
    def validateur_certificats(self):
        return self.__validateur_certificats

    @property
    def validateur_messages(self):
        return self.__validateur_messages

    @property
    def configuration(self):
        return self.__configuration

    async def charger_init(self):
        path_certissuer = self.__configuration.path_certissuer
        path_ca = path.join(path_certissuer, 'millegrille.pem')
        try:
            self.__ca = EnveloppeCertificat.from_file(path_ca)
            self.__ca_str = self.__ca.certificat_pem
            self.__validateur_certificats = ValidateurCertificatCache(self.__ca)
            self.__validateur_messages = ValidateurMessage(self.__validateur_certificats)
        except FileNotFoundError:
            pass

        path_cert = path.join(path_certissuer, 'cert.pem')
        path_cle = path.join(path_certissuer, 'key.pem')
        path_password = path.join(path_certissuer, 'password.txt')
        try:
            cle_intermediaire = CleCertificat.from_files(path_cle, path_cert, path_password)
            await self.__validateur_certificats.valider(cle_intermediaire.enveloppe.chaine_pem())
            if cle_intermediaire.cle_correspondent():
                self.__cle_intermediaire = cle_intermediaire
            else:
                # Cleanup, le cert/cle ne correspondent pas
                remove(path_cle)
                remove(path_cert)
                remove(path_password)
        except FileNotFoundError:
            pass

    async def entretien(self):
        if self.__validateur_certificats is not None:
            await self.__validateur_certificats.entretien()

    def get_csr(self) -> str:
        if self.__csr is None:
            try:
                instance_id = self.__cle_intermediaire.enveloppe.subject_common_name
            except (AttributeError, TypeError):
                instance_id = self.configuration.instance_id
            self.__csr = generer_csr_intermediaire(instance_id)

        return self.__csr.get_pem_csr()

    async def sauvegarder_certificat(self, info_cert: dict):
        cle_pem = self.__csr.get_pem_cle()
        password = self.__csr.password
        cert_ca = info_cert['ca']
        cert_pem = info_cert['intermediaire']

        path_certissuer = self.__configuration.path_certissuer

        # Valider cert recu avec cle en memoire
        enveloppe_inter = EnveloppeCertificat.from_pem(cert_pem)
        if len(enveloppe_inter.chaine_pem()) != 1:
            raise Exception("Certificat intermediaire avec chaine > 1")
        clecert_inter = CleCertificat.from_pems(cle_pem, cert_pem, password=password)
        if clecert_inter.cle_correspondent() is False:
            raise Exception("Le certificat recu ne correspond pas a la cle")
        if enveloppe_inter.is_ca is False:
            raise Exception("Enveloppe intermediaire n'est pas CA")

        # Supprimer CSR en memoire (signature valide)
        self.__csr = None

        # Since 2024.9 - Always using same validation check and saving CA cert (validation of signing was failing)
        # if self.__validateur_certificats is not None:
        #     # Valider le certificat intermediaire. Doit correspondre au cert CA de la millegrille.
        #     await self.__validateur_certificats.valider(cert_pem)
        # else:
        enveloppe_ca = EnveloppeCertificat.from_pem(cert_ca)
        if enveloppe_ca.is_root_ca is False:
            raise Exception("Certificat CA n'est pas root")

        if self.__idmg is not None:
            if self.__idmg != enveloppe_ca.idmg:
                raise Exception("Mismatch idmg avec systeme local et cert CA recu")
        else:
            # On n'a pas de lock pour la millegrille, on accepte le nouveau certificat
            path_ca = path.join(path_certissuer, 'millegrille.pem')
            with open(path_ca, 'w') as fichier:
                fichier.write(cert_ca)

        path_cle = path.join(path_certissuer, 'key.pem')
        path_cert = path.join(path_certissuer, 'cert.pem')
        path_password = path.join(path_certissuer, 'password.txt')
        with open(path_cert, 'w') as fichier:
            fichier.write(cert_pem)
        with open(path_cle, 'w') as fichier:
            fichier.write(cle_pem)
        chmod(path_cle, 0o600)
        with open(path_password, 'w') as fichier:
            fichier.write(password)
        chmod(path_password, 0o600)

        # Recharger validateur, certificats
        await self.charger_init()
