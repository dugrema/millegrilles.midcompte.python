#!/usr/bin/env bash

docker run --name zookeeper1 --rm -t \
  -p 9983:2181 \
  --mount type=volume,source=solr-zookeeper-data,target=/data \
  --mount type=volume,source=solr-zookeeper-datalog,target=/datalog \
  zookeeper:3.8
