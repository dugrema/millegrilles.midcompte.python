#!/bin/env bash
PATH_MILLEGRILLES=/var/opt/millegrilles

# Importer instance_id
VARS=`$PATH_MILLEGRILLES/bin/read_config.py`
source $PATH_MILLEGRILLES/bin/source_config.sh

export INSTANCE_ID
echo "Demarrer certissuer pour instance $INSTANCE_ID"

source /var/opt/millegrilles/venv/bin/activate
export PYTHONPATH="${PYTHONPATH}:/var/opt/millegrilles/python"
python3 -m millegrilles_certissuer $@

