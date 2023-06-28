/usr/bin/env bash

echo Installer media

apt update
apt install -y libmagickwand-dev ghostscript
# Utiliser unoconv pour supporter conversion documents (word, wordperfect, etc)
# apt install unoconv

pip3 install "wand>=0.6,<0.7"

