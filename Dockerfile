FROM docker.maple.maceroc.com:5000/millegrilles_messages_python:2023.6.9

ENV BUILD_FOLDER=/opt/millegrilles/build \
    BUNDLE_FOLDER=/opt/millegrilles/dist \
    PYTHONPATH=/opt/millegrilles/dist \
    SRC_FOLDER=/opt/millegrilles/build/src \
    CA_PEM=/run/secrets/pki.millegrille.cert \
    MQ_HOSTNAME=mq, \
    MQ_PORT=5673, \
    MG_REDIS_HOST=redis \
    MG_REDIS_PORT=6379 \
    CERT_PEM=/run/secrets/cert.pem \
    KEY_PEM=/run/secrets/key.pem

COPY . $BUILD_FOLDER

WORKDIR /opt/millegrilles/build

RUN pip3 install --no-cache-dir -r $BUILD_FOLDER/requirements.txt && \
    python3 ./setup.py install

WORKDIR /opt/millegrilles/dist

CMD ["-m", "millegrilles_solr", "--verbose"]
