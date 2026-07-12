import os
import pathlib
import requests

from urllib.parse import urlparse

from millegrilles_messages.messages import Constantes

from millegrilles_messages.bus.BusContext import load_message_formatter
from millegrilles_messages.certificats.Generes import CleCsrGenere
from millegrilles_messages.messages.EnveloppeCertificat import EnveloppeCertificat
from millegrilles_messages.messages.CleCertificat import CleCertificat

ROOT_PATH = pathlib.Path(os.environ.get("MILLEGRILLES_ROOT") or "/tmp/millegrilles_dev1")

def load_manager_certs():
    manager_pem_path = ROOT_PATH.joinpath("secrets/manager.pem")
    ca_pem_path = ROOT_PATH.joinpath("etc/millegrille.pem")

    manager = CleCertificat.from_files(manager_pem_path, manager_pem_path)
    ca = EnveloppeCertificat.from_file(str(ca_pem_path))

    signateur, formatteur = load_message_formatter(manager, ca)

    return manager, ca, signateur, formatteur

def signer_module(manager_cert, request_message, formatteur_message):
    instance_id = manager_cert.enveloppe.subject_common_name
    idmg = manager_cert.enveloppe.subject_organization_name
    cle_csr = CleCsrGenere.build(instance_id, idmg)
    csr_str = cle_csr.get_pem_csr()

    request_message['csr'] = csr_str

    # Demander un nouveau certificat. Timeout long (60 secondes).
    message_signe, _uuid = formatteur_message.signer_message(Constantes.KIND_DOCUMENT, request_message)

    url_issuer = 'http://localhost:2080/signerModule'
    response = requests.post(url_issuer, json=message_signe)
    response.raise_for_status()
    response_message = response.json()
    certificat = response_message['certificat']

    return certificat

def test_sign_nginx():
    manager_cert, ca_cert, signateur, formatteur_message = load_manager_certs()

    request_message = {'roles': ['nginx'], "dns": {"localhost": True, "hostnames": ["devmachine.local"], "domain": True}}
    certificat = signer_module(manager_cert, request_message, formatteur_message)
    cert_str = "\n".join(certificat)
    print(f"\n{cert_str}")

    # Checks
    enveloppe = EnveloppeCertificat.from_pem(cert_str)
    if enveloppe.subject_common_name != manager_cert.enveloppe.subject_common_name:
        raise Exception("INSTANCE_ID mismatch")
    if 'nginx' not in enveloppe.get_roles:
        raise Exception("nginx role not in certificate")
    if enveloppe.subject_organizational_unit_name != "nginx":
        raise Exception("nginx should be the certificate OU")
    if enveloppe.subject_organization_name != manager_cert.enveloppe.subject_organization_name:
        raise Exception("wrong IDMG")

def test_sign_core():
    manager_cert, ca_cert, signateur, formatteur_message = load_manager_certs()

    request_message = {
        'roles': ['core'],
        'exchanges': ['4.secure', '3.protege', '2.prive', '1.public'],
        'domaines': ["CoreBackup", "CoreCatalogues", "CoreMaitreDesComptes", "CorePki", "CoreTopologie"],
        'dns': {"hostnames": ["core"]}
    }

    certificat = signer_module(manager_cert, request_message, formatteur_message)
    cert_str = "\n".join(certificat)
    print(f"\n{cert_str}")

    # Checks
    enveloppe = EnveloppeCertificat.from_pem(cert_str)
    if enveloppe.subject_common_name != manager_cert.enveloppe.subject_common_name:
        raise Exception("INSTANCE_ID mismatch")
    if 'core' not in enveloppe.get_roles:
        raise Exception("nginx role not in certificate")
    if '4.secure' not in enveloppe.get_exchanges:
        raise Exception("4.secure exchange not in certificate")
    if enveloppe.subject_organizational_unit_name != "core":
        raise Exception("core should be the certificate OU")
    if enveloppe.subject_organization_name != manager_cert.enveloppe.subject_organization_name:
        raise Exception("wrong IDMG")
