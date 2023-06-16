#!/usr/bin/env bash

KEYSTORE_PATH=/tmp/keystore/solr-ssl.keystore.p12

mkdir /tmp/keystore
chmod 700 /tmp/keystore

# Importer certificat de millegrille (CA)
keytool -import -trustcacerts \
  -storepass secret \
  -alias millegrille \
  -file /run/secrets/pki.millegrille.cert \
  -keystore "${KEYSTORE_PATH}" \
  -noprompt

# Importer certificat avec cle
cat /run/secrets/pki.fichiers.cert /run/secrets/pki.millegrille.cert > /tmp/keystore/cert.pem
openssl pkcs12 -export \
  -in /tmp/keystore/cert.pem -inkey /run/secrets/pki.fichiers.cle \
  -name solrserver \
  -passout pass:secret \
  > /tmp/keystore/solrserver.p12

keytool -importkeystore \
  -srckeystore /tmp/keystore/solrserver.p12 \
  -srcstorepass secret \
  -srcstoretype pkcs12 \
  -destkeystore "${KEYSTORE_PATH}" \
  -deststorepass secret \
  -alias solrserver

echo "--------------------------"
keytool -list -storepass secret -keystore ${KEYSTORE_PATH}
echo "--------------------------"
