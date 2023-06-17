#!/usr/bin/env bash

docker run --name zookeeper1 --rm -t \
  -p 9983:2181 \
  --hostname solrzookeeper \
  --mount type=volume,source=solr-zookeeper-data,target=/data \
  --mount type=volume,source=solr-zookeeper-datalog,target=/datalog \
  --mount type=bind,source=/var/opt/millegrilles,target=/var/opt/millegrilles \
  --env MG_KEY=/var/opt/millegrilles/secrets/pki.solr_zookeeper.cle \
  --env MG_CERT=/var/opt/millegrilles/secrets/pki.solr_zookeeper.cert \
  --env MG_CA=/var/opt/millegrilles/configuration/pki.millegrille.cert \
  --network millegrille_net --network-alias solrzookeeper \
  zookeeper:3.8

#  mg_zookeeper
