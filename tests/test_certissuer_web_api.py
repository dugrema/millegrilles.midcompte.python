import os
import subprocess
import time
import json
import tempfile
import shutil
import unittest
import requests
import datetime

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
        
        # 5. Start Certissuer
        cls.process = subprocess.Popen(
            ["/tmp/millegrilles_dev1/venv/bin/python", "-m", "millegrilles_certissuer", "--verbose"],
            env=cls.env,
            stdout=open(os.path.join(cls.test_dir, "certissuer.log"), "w"),
            stderr=open(os.path.join(cls.test_dir, "certissuer.err"), "w"),
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
        shutil.rmtree(cls.test_dir)

    def test_get_csr(self):
        response = requests.get(f"{self.base_url}/csr")
        self.assertEqual(response.status_code, 200)
        self.assertIn("BEGIN CERTIFICATE REQUEST", response.text)

    def test_installer(self):
        subprocess.run(["openssl", "genpkey", "-algorithm", "ED25519", "-out", "test_inst.key"], check=True)
        subprocess.run(["openssl", "req", "-new", "-key", "test_inst.key", "-out", "test_inst.csr", "-subj", "/CN=my-instance"], check=True)
        with open("test_inst.csr", "r") as f:
            csr_pem = f.read()
        with open(os.path.join(self.etc_dir, "millegrille.pem"), "r") as f:
            ca_cert_pem = f.read()
        subprocess.run(["openssl", "genpkey", "-algorithm", "ED25519", "-out", "test_int.key"], check=True)
        subprocess.run(["openssl", "req", "-new", "-key", "test_int.key", "-out", "test_int.csr", "-subj", "/CN=test-int"], check=True)
        subprocess.run([
            "openssl", "x509", "-req", "-in", "test_int.csr", "-CA", os.path.join(self.etc_dir, "millegrille.pem"),
            "-CAkey", os.path.join(self.etc_dir, "millegrille.key"), "-out", "test_int.crt", "-days", "365"
        ], check=True)
        with open("test_int.crt", "r") as f:
            int_cert_pem = f.read()

        payload = {
            "csr": csr_pem,
            "securite": "PROTEGE",
            "ca": ca_cert_pem,
            "intermediaire": int_cert_pem
        }
        
        response = requests.post(f"{self.base_url}/installer", json=payload)
        self.assertEqual(response.status_code, 201)
        
        os.remove("test_inst.key")
        os.remove("test_inst.csr")
        os.remove("test_int.key")
        os.remove("test_int.csr")
        os.remove("test_int.crt")

    def test_signer_usager(self):
        subprocess.run(["openssl", "genpkey", "-algorithm", "ED25519", "-out", "test_user.key"], check=True)
        subprocess.run(["openssl", "req", "-new", "-key", "test_user.key", "-out", "test_user.csr", "-subj", "/CN=user-cn"], check=True)
        with open("test_user.csr", "r") as f:
            csr_pem = f.read()
        
        payload = {
            "contenu": json.dumps({
                "csr": csr_pem,
                "nom_usager": "user-cn",
                "user_id": "user-123"
            })
        }
        response = requests.post(f"{self.base_url}/signerUsager", json=payload)
        self.assertIn(response.status_code, [200, 403, 401])
        os.remove("test_user.key")
        os.remove("test_user.csr")

if __name__ == "__main__":
    unittest.main()
