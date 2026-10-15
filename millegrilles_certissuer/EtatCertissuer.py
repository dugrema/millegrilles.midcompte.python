import logging
import tempfile
from os import path, remove, chmod
from typing import Optional

from millegrilles_certissuer.Configuration import ConfigurationCertissuer
...
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
        # self.__csr: Optional[CleCsrGenere] = None

        # self.__idmg: Optional[str] = None
        # self.__ca: Optional[EnveloppeCertificat] = None
        # self.__cle_intermediaire: Optional[CleCertificat] = None
        self.__validateur_certificats: Optional[ValidateurCertificatCache] = None
        self.__validateur_messages: Optional[ValidateurMessage] = None

        self.__ca_str: Optional[str] = None

    # @property
    # def ca_str(self):
    #     return self.__ca_str

    # @property
    # def cle_intermediaire(self):
    #     return self.__cle_intermediaire

    # @property
    # def validateur_certificats(self):
    #     return self.__validateur_certificats

    # @property
    # def validateur_messages(self):
    #     return self.__validateur_messages

    @property
    def configuration(self):
        return self.__configuration

    # async def charger_init(self):
    #     path_certissuer = self.__configuration.path_certissuer
    #     path_ca = self.__configuration.path_ca
    #     try:
    #         self.__ca = EnveloppeCertificat.from_file(path_ca)
    #         self.__ca_str = self.__ca.certificat_pem
    #         self.__validateur_certificats = ValidateurCertificatCache(self.__ca)
    #         self.__validateur_messages = ValidateurMessage(self.__validateur_certificats)
    #     except FileNotFoundError:
    #         raise RuntimeError(f"Le certificat CA est introuvable à l'emplacement: {path_ca}")
    #     except Exception as e:
    #         raise RuntimeError(f"Erreur lors du chargement du certificat CA: {str(e)}")
    #
    #     if self.__configuration.signing_cert_path and path.exists(self.__configuration.signing_cert_path):
    #         try:
    #             cle_intermediaire = CleCertificat.from_files(self.__configuration.signing_cert_path, self.__configuration.signing_cert_path, password=None)
    #             if self.__validateur_certificats is not None:
    #                 await self.__validateur_certificats.valider(cle_intermediaire.enveloppe.chaine_pem())
    #             if cle_intermediaire.cle_correspondent():
    #                 self.__cle_intermediaire = cle_intermediaire
    #             else:
    #                 self.__logger.error("Le certificat de signature ne correspond pas à la clé")
    #         except Exception as e:
    #             self.__logger.error("Erreur lors du chargement du certificat de signature: %s" % str(e))


    async def entretien(self):
        if self.__validateur_certificats is not None:
            await self.__validateur_certificats.entretien()
