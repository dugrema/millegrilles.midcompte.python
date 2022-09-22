from millegrilles_certissuer.Configuration import ConfigurationWeb
from millegrilles_certissuer.EtatCertissuer import EtatCertissuer
from millegrilles_messages.certificats.CertificatsConfiguration import signer_configuration
from millegrilles_messages.certificats.CertificatsInstance import \
    signer_instance_secure, signer_instance_protege, signer_instance_prive, signer_instance_public
from millegrilles_messages.certificats.CertificatsUsager import signer_usager
from millegrilles_messages.messages import Constantes
from millegrilles_messages.certificats.Generes import DUREE_CERT_DEFAUT


class CertificatHandler:

    def __init__(self, configuration: ConfigurationWeb, etat_certissuer: EtatCertissuer):
        self.__configuration = configuration
        self.__etat_certissuer = etat_certissuer

    def generer_certificat_instance(self, csr: str, securite: str, duree=DUREE_CERT_DEFAUT) -> str:
        cle_intermediaire = self.__etat_certissuer.cle_intermediaire
        if securite == Constantes.SECURITE_SECURE:
            enveloppe_certificat = signer_instance_secure(cle_intermediaire, csr, duree)
        elif securite == Constantes.SECURITE_PROTEGE:
            enveloppe_certificat = signer_instance_protege(cle_intermediaire, csr, duree)
        elif securite == Constantes.SECURITE_PRIVE:
            enveloppe_certificat = signer_instance_prive(cle_intermediaire, csr, duree)
        elif securite == Constantes.SECURITE_PUBLIC:
            enveloppe_certificat = signer_instance_public(cle_intermediaire, csr, duree)
        else:
            raise Exception('Type securite %s non supporte pour une instance' % securite)
        return enveloppe_certificat.chaine_pem()

    def generer_certificat_module(self, parametres: dict) -> str:
        cle_intermediaire = self.__etat_certissuer.cle_intermediaire
        csr = parametres['csr']
        enveloppe = signer_configuration(cle_intermediaire, csr, parametres)
        return enveloppe.chaine_pem()

    def generer_certificat_usager(self, parametres: dict) -> str:
        cle_intermediaire = self.__etat_certissuer.cle_intermediaire
        csr = parametres['csr']
        enveloppe = signer_usager(cle_intermediaire, csr, parametres)

        # Ajouter le certificat CA
        ca_str = self.__etat_certissuer.ca_str
        chaine_pem = enveloppe.chaine_pem().copy()
        chaine_pem.append(ca_str)

        return chaine_pem
