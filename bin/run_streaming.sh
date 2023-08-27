#!/usr/bin/env bash

docker run --rm -it \
  -p 8983:8983 \
  --hostname stream \
  --mount type=volume,source=millegrilles-staging,target=/var/opt/millegrilles/staging \
  --mount type=bind,source=/home/mathieu/tmp/certs,target=/run/secrets \
  --network millegrille_net \
  stream