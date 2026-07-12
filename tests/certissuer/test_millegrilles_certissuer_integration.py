import os
import subprocess
import tempfile
import time
import pytest

def test_certissuer_integration_success(tmp_path):
    # 1. Set up directory structure
    etc_dir = tmp_path / "etc"
    secrets_dir = tmp_path / "secrets" / "certissuer"
    etc_dir.mkdir()
    secrets_dir.mkdir(parents=True)

    # 2. Generate a CA (self-signed)
    ca_key = etc_dir / "millegrille.key"
    ca_pem = etc_dir / "millegrille.pem"
    subprocess.run([
        "openssl", "req", "-x509", "-newkey", "rsa:2048", "-keyout", str(ca_key),
        "-out", str(ca_pem), "-days", "365", "-nodes", "-subj", "/CN=MilleGrille CA"
    ], check=True)

    # 3. Generate a certissuer Key and CSR (encrypted with 'password')
    certissuer_key = secrets_dir / "key.pem"
    certissuer_csr = secrets_dir / "csr.pem"
    subprocess.run([
        "openssl", "genrsa", "-aes256", "-passout", "pass:password", "-out", str(certissuer_key), "2048"
    ], check=True)
    subprocess.run([
        "openssl", "req", "-new", "-key", str(certissuer_key), "-out", str(certissuer_csr),
        "-subj", "/CN=CertIssuer", "-passin", "pass:password"
    ], check=True)

    # 4. Sign the CertIssuer CSR with the CA
    certissuer_pem = secrets_dir / "cert.pem"
    subprocess.run([
        "openssl", "x509", "-req", "-in", str(certissuer_csr), "-CA", str(ca_pem),
        "-CAkey", str(ca_key), "-out", str(certissuer_pem), "-days", "365"
    ], check=True)

    # 5. Create the password file
    password_file = secrets_dir / "password.txt"
    password_file.write_text("password")
    password_file.chmod(0o600)

    # 6. Set environment variables
    env = os.environ.copy()
    env["MILLEGRILLES_ROOT"] = str(tmp_path)
    env["CERTISSUER_PATH"] = str(secrets_dir)
    
    # 7. Run the application in a subprocess
    cmd = [
        "/tmp/millegrilles_dev1/venv/bin/python", "-m", "millegrilles_certissuer", "--verbose"
    ]
    
    process = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    try:
        # 8. Wait for it to start (give it some time)
        time.sleep(5)
        
        # Check if it's still running
        assert process.poll() is None, "Process failed to start"

        # 9. Terminate the process gracefully
        process.terminate()
        stdout, stderr = process.communicate(timeout=10)

        # 10. Assertions
        assert process.returncode in [0, -15], f"Process exited with code {process.returncode}"

    except Exception as e:
        if 'process' in locals() and process.poll() is None:
            process.terminate()
        raise e
