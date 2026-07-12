# MilleGrilles Midcompte Project

This repository contains several independent Python applications for the MilleGrilles system.

## Architecture & Entrypoints
- **Structure**: Multi-package repository. Each application is a standalone module.
- **Run Applications**: Use `python -m <package_name>` (e.g., `python -m millegrilles_midcompte`).
- **Main Packages**:
  - `millegrilles_ceduleur`
  - `millegrilles_certissuer`
  - `millegrilles_media`
  - `millegrilles_midcompte`
  - `millegrilles_relaiweb`
  - `millegrilles_solr`
  - `millegrilles_streaming`

## Development Environment
- **Python Version**: 3.13
- **Core Dependencies**: `pytz`, `pymongo`, `aiohttp`, `requests`, `pyjwt`.
- **Required Services**: Many modules require external services (MongoDB, RabbitMQ, Redis, Solr, Zookeeper) typically run via Docker.
- **Configuration**: Most modules require specific environment variables for certificates (`CERT_PEM`, `KEY_PEM`, `CA_PEM`) and connections (`MQ_HOSTNAME`, `MONGO_HOSTNAME`, `REDIS_HOSTNAME`, etc.).
  - For detailed environment variable requirements, refer to `doc/env.dev.md`.
  - Some modules require specific IDs (e.g., `INSTANCE_ID` for `millegrilles_certissuer`).
- **Environment Note**: `millegrilles_messages` is required and is included in the virtual environment when running outside Docker (e.g., for dev/testing). Ensure the venv is loaded when running any application.

## Debugging & Testing
- **Verbose Mode**: Many modules support a `--verbose` flag for increased logging.
- **Testing**: Tests are located in the `tests/` directory.
