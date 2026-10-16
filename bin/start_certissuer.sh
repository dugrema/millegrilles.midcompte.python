#!/bin/env bash
source .venv/bin/activate

export WEB_PORT=2080
export SIGNING_PATH=$(pwd)/hosting

echo "Signing path: $SIGNING_PATH"
mkdir -p "$SIGNING_PATH"

python3 -m millegrilles_certissuer
