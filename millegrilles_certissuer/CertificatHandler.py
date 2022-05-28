from millegrilles_certissuer.Configuration import ConfigurationWeb
from millegrilles_certissuer.EtatCertissuer import EtatCertissuer
from millegrilles_messages.certificats.CertificatsConfiguration import signer_configuration
from millegrilles_messages.certificats.CertificatsInstance import signer_instance_protege
from millegrilles_messages.messages import Constantes


class CertificatHandler:

    def __init__(self, configuration: ConfigurationWeb, etat_certissuer: EtatCertissuer):
        self.__configuration = configuration
        self.__etat_certissuer = etat_certissuer

    def generer_certificat_instance(self, csr: str, securite: str) -> str:
        cle_intermediaire = self.__etat_certissuer.cle_intermediaire
        if securite == Constantes.SECURITE_PROTEGE:
            enveloppe_certificat = signer_instance_protege(cle_intermediaire, csr)
        else:
            raise Exception('Type securite %s non supporte pour une instance' % securite)
        return enveloppe_certificat.chaine_pem()

    def generer_certificat_module(self, parametres: dict) -> str:
        cle_intermediaire = self.__etat_certissuer.cle_intermediaire
        csr = parametres['csr']
        enveloppe = signer_configuration(cle_intermediaire, csr, parametres)
        return enveloppe.chaine_pem()
