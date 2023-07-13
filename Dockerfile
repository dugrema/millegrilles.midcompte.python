FROM docker.maple.maceroc.com:5000/millegrilles_messages_python:2023.7.0

ENV BUILD_FOLDER=/opt/millegrilles/build \
    BUNDLE_FOLDER=/opt/millegrilles/dist \
    PYTHONPATH=/opt/millegrilles/dist \
    SRC_FOLDER=/opt/millegrilles/build/src \
    CERT_PEM=/run/secrets/cert.pem \
    KEY_PEM=/run/secrets/key.pem \
    CA_PEM=/run/secrets/pki.millegrille.cert \
    MQ_URL=https://mq:8443 \
    MQ_HOSTNAME=mq \
    MQ_PORT=5673 \
    MG_REDIS_HOST=redis \
    MG_REDIS_PORT=6379

COPY . $BUILD_FOLDER

WORKDIR /opt/millegrilles/build

RUN pip3 install --no-cache-dir -r $BUILD_FOLDER/requirements.txt && \
    python3 ./setup.py install

WORKDIR /opt/millegrilles/dist

CMD ["-m", "millegrilles_midcompte", "--verbose"]
