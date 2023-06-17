#!/usr/bin/env bash

#docker run --rm -t \
#  -p 8983:8983 \
#  --mount type=volume,source=solr-data,target=/var/solr \
#  --mount type=bind,source=/home/mathieu/tmp/certs,target=/run/secrets \
#  --env SOLR_SSL_ENABLED=true \
#  --env SOLR_SSL_KEY_STORE=/run/secrets/solr-ssl.keystore.p12 \
#  --env SOLR_SSL_KEY_STORE_PASSWORD=secret \
#  --env SOLR_SSL_TRUST_STORE=/run/secrets/solr-ssl.keystore.p12 \
#  --env SOLR_SSL_TRUST_STORE_PASSWORD=secret \
#  --env SOLR_SSL_NEED_CLIENT_AUTH=true \
#  --env SOLR_SSL_WANT_CLIENT_AUTH=true \
#  --env SOLR_SSL_CHECK_PEER_NAME=true \
#  solr:9.2 -c -z thinkcentre1.maple.maceroc.com:9983

docker run --rm -it \
  -p 8983:8983 \
  --hostname solr \
  --mount type=volume,source=solr-data,target=/var/solr \
  --mount type=bind,source=/home/mathieu/tmp/certs,target=/run/secrets \
  --env ZOOKEEPER_URL=solrzookeeper:2181 \
  --network millegrille_net \
  mgsolr