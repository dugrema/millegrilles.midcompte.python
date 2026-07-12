# millegrilles_certissuer

This module is responsible for issuing certificates within the MilleGrilles system.

## Running the Certissuer

To run the `certissuer` module, use the following command. It is recommended to use a `timeout` to prevent the process from hanging indefinitely during testing.

```bash
# Using the development virtual environment
timeout 20s /tmp/millegrilles_dev1/venv/bin/python -m millegrilles_certissuer --verbose
```

### Environment Requirements

The module requires certain environment variables to be set, typically provided by the `config.env` file in the `MILLEGRILLES_ROOT` directory.

**Key Environment Variables:**
- `INSTANCE_ID`: A unique identifier for the instance.
- `CA_CERT_PATH`: The MilleGrille's CA file under `$MILLEGRILLES_ROOT/etc/millegrille.pem`.
- `CERTISSUER_PATH`: The directory where certificates are stored.
    - Default (Docker): `/var/opt/millegrilles/certissuer`
    - Dev/Test: Usually relative to `$MILLEGRILLES_ROOT/secrets/certissuer`
- `SIGNING_CERT_PATH`: Path to a single PEM file containing both the certificate and the private key (used for development/testing).

**Running with an environment file:**
If you are running outside of the established environment, you may need to export the variables from `config.env` first:
```bash
export $(grep -v '^#' /tmp/millegrilles_dev1/config.env | xargs)
# Optional: Set signing cert for dev
export SIGNING_CERT_PATH=/tmp/millegrilles_dev1/secrets/certissuer/signing_ca.pem

timeout 20s /tmp/millegrilles_dev1/venv/bin/python -m millegrilles_certissuer --verbose
```

### Debugging

Use the `--verbose` flag to enable detailed logging for both the `millegrilles_certissuer` and `millegrilles_messages` modules.
