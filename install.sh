#!/bin/env bash

PATH_VENV=$1

echo "Activer venv ${PATH_VENV}"
source "${PATH_VENV}/bin/activate"

echo "Installer pip wheel"
pip3 install wheel

#echo "Installer millegrilles messages (path $URL_MGMESSAGES)"
#pip3 install $URL_MGMESSAGES

pip3 install -r requirements.txt
python3 setup.py install

echo "[INFO] Fin installation midcompte"
