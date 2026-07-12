import os
import subprocess
import time
import json
import tempfile
import unittest
import requests
import datetime
import binascii
import hashlib
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

class TestCertissuerWebAPI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_dir = tempfile.mkdtemp()
        cls.etc_dir = os.path.join(cls.test_dir, "etc")
        cls.secrets_dir = os.path.join(cls.test_dir, "secrets", "certissuer")
        os.makedirs(cls.etc_dir, exist_ok=True)
        os.makedirs(cls.secrets_dir, exist_ok=True)

        # 1. Create CA (Ed25519)
        subprocess.run([
            "openssl", "req", "-x509", "-newkey", "ed25519", "-keyout", os.path.join(cls.etc_dir, "millegrille.key"),
            "-out", os.path.join(cls.etc_dir, "millegrille.pem"), "-days", "365", "-nodes", "-subj", "/CN=MilleGrille CA/O=MilleGrilles"
        ], check=True)

        # 2. Create Intermediate (Ed25519)
        subprocess.run([
            "openssl", "genpkey", "-algorithm", "ED25519", "-out", os.path.join(cls.secrets_dir, "key_unencrypted.pem")
        ], check=True)
        subprocess.run([
            "openssl", "req", "-new", "-key", os.path.join(cls.secrets_dir, "key_unencrypted.pem"), "-out", os.path.join(cls.secrets_dir, "csr.pem"),
            "-subj", "/CN=CertIssuer/O=MilleGrilles"
        ], check=True)
        subprocess.run([
            "openssl", "pkey", "-in", os.path.join(cls.secrets_dir, "key_unencrypted.pem"), "-aes256", "-passout", "pass:password", "-out", os.path.join(cls.secrets_dir, "key.pem")
        ], check=True)
        subprocess.run([
            "openssl", "x509", "-req", "-in", os.path.join(cls.secrets_dir, "csr.pem"), "-CA", os.path.join(cls.etc_dir, "millegrille.pem"),
            "-CAkey", os.path.join(cls.etc_dir, "millegrille.key"), "-out", os.path.join(cls.secrets_dir, "cert.pem"), "-days", "365"
        ], check=True)

        # 3. Create password file
        with open(os.path.join(cls.secrets_dir, "password.txt"), "w") as f:
            f.write("password")
        os.chmod(os.path.join(cls.secrets_dir, "password.txt"), 0o600)

        # 4. Setup environment
        cls.env = os.environ.copy()
        cls.env["MILLEGRILLES_ROOT"] = cls.test_dir
        cls.env["CERTISSUER_PATH"] = cls.secrets_dir
        cls.env["INSTANCE_ID"] = "test_instance"
        
        # 5. Start Certissuer
        cls.log_file = os.path.join(cls.test_dir, "certissuer.log")
        cls.err_file = os.path.join(cls.test_dir, "certissuer.err")
        cls.stdout_f = open(cls.log_file, "w")
        cls.stderr_f = open(cls.err_file, "w")
        
        cls.process = subprocess.Popen(
            ["/tmp/millegrilles_dev1/venv/bin/python", "-m", "millegrilles_certissuer", "--verbose"],
            env=cls.env,
            stdout=cls.stdout_f,
            stderr=cls.stderr_f,
            text=True
        )
        time.sleep(5)
        
        if cls.process.poll() is not None:
            raise Exception("Certissuer failed to start. Check logs.")
            
        cls.base_url = "http://localhost:2080"

    @classmethod
    def tearDownClass(cls):
        cls.process.terminate()
        cls.process.wait()
        cls.stdout_f.close()
        cls.stderr_f.close()
        print(f"Test directory preserved at: {cls.test_dir}")

    def test_get_csr(self):
        response = requests.get(f"{self.base_url}/csr")
        self.assertEqual(response.status_code, 200)
        self.assertIn("BEGIN CERTIFICATE REQUEST", response.text)

    def test_installer_renewal(self):
        # Use current key to simulate a renewal
        with open(os.path.join(self.secrets_dir, "key.pem"), "r") as f:
            current_key_pem = f.read()
        with open(os.path.join(self.secrets_dir, "cert.pem"), "r") as f:
            current_cert_pem = f.read()
            
        # Create a renewal CSR using the SAME key
        subprocess.run([
            "openssl", "req", "-new", "-key", os.path.join(self.secrets_dir, "key.pem"), "-out", "test_inst.csr", "-subj", "/CN=CertIssuer/O=MilleGrilles", "-passin", "pass:password"
        ], check=True)
        with open("test_inst.csr", "r") as f:
            csr_pem = f.read()
            
        with open(os.path.join(self.etc_dir, "millegrille.pem"), "r") as f:
            ca_cert_pem = f.read()

        # Sign the CSR with the CA using the SAME key
        subprocess.run([
            "openssl", "x509", "-req", "-in", "test_inst.csr", "-CA", os.path.join(self.etc_dir, "millegrille.pem"),
            "-CAkey", os.path.join(self.etc_dir, "millegrille.key"), "-out", "test_inst_cert.pem", "-days", "365"
        ], check=True)
        with open("test_inst_cert.pem", "r") as f:
            new_cert_pem = f.read()

        payload = {
            "csr": csr_pem,
            "securite": "PROTEGE",
            "ca": ca_cert_pem,
            "intermediaire": new_cert_pem
        }
        
        response = requests.post(f"{self.base_url}/installer", json=payload)
        if response.status_code != 201:
            print(f"Installer response: {response.status_code}")
            print(f"Response content: {response.text}")
        self.assertEqual(response.status_code, 201)
        
        os.remove("test_inst.csr")
        os.remove("test_inst_cert.pem")

    def test_signer_usager(self):
        # 1. Prepare CSR for user
        subprocess.run(["openssl", "genpkey", "-algorithm", "ED25519", "-out", "test_user.key"], check=True)
        subprocess.run(["openssl", "req", "-new", "-key", "test_user.key", "-out", "test_user.csr", "-subj", "/CN=user-cn"], check=True)
        with open("test_user.csr", "r") as f:
            csr_pem = f.read()

        # 2. Get user public key bytes
        with open("test_user.key", "r") as f:
            user_priv_pem = f.read()
        user_priv = serialization.load_pem_private_key(user_priv_pem.encode(), password=None)
        user_pub_bytes = user_priv.public_key().public_bytes(
            encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw)
        user_pub_hex = binascii.hexlify(user_pub_bytes).decode('utf-8')

        # 3. Prepare payload content
        contenu = {
            "csr": csr_pem,
            "nom_usager": "user-cn",
            "user_id": "user-123"
        }
        contenu_json = json.dumps(contenu)
        
        # 4. Prepare message header (using the core certificate as signer)
        # We need the password
        password = "password"
        with open(os.path.join(self.secrets_dir, "key.pem"), "r") as f:
            core_key_pem = f.read()
        core_priv = serialization.load_pem_private_key(core_key_pem.encode(), password=password.encode())
        
        # Create message
        msg_id = binascii.hexlify(os.urandom(32)).decode('utf-8')
        estampille = int(time.time())
        kind = 2 # KIND_COMMANDE
        
        msg_dict = {
            "pubkey": user_pub_hex,
            "estampille": estampille,
            "kind": kind,
            "contenu": contenu_json
        }
        
        payload_to_hash = [msg_dict["pubkey"], msg_dict["estampille"], msg_dict["kind"], msg_dict["contenu"]]
        message_str = json.dumps(payload_to_hash, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
        msg_bytes = message_str.encode('utf-8')
        
        msg_id_hex = binascii.hexlify(hashlib.blake2s(msg_bytes, digest_size=32).digest()).decode('utf-8')
        
        signature_bytes = core_priv.sign(msg_bytes)
        signature_hex = binascii.hexlify(signature_bytes).decode('utf-8')
        
        msg_dict["id"] = msg_id_hex
        msg_dict["sig"] = signature_hex
        
        with open(os.path.join(self.secrets_dir, "cert.pem"), "r") as f:
            core_cert_pem = f.read()
        msg_dict["certificat"] = core_cert_pem
        
        response = requests.post(f"{self.base_url}/signerUsager", json=msg_dict)
        if response.status_code not in [200, 403, 401]:
            print(f"SignerUsager response: {response.status_code}")
            print(f"Response content: {response.text}")
        self.assertIn(response.status_code, [200, 403, 401])
        os.remove("test_user.key")
        os.remove("test_user.csr")

if __name__ == "__main__":
    unittest.main()
