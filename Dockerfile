FROM registry.millegrilles.com/millegrilles/messages_python:2026.3.8 AS stage1

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
    MG_REDIS_PORT=6379 \
    WEB_PORT=1443

WORKDIR /opt/millegrilles/build

COPY requirements.txt $BUILD_FOLDER/requirements.txt

RUN pip3 install --no-cache-dir -r $BUILD_FOLDER/requirements.txt && \
    mkdir -p /var/opt/millegrilles/staging

FROM stage1

COPY . $BUILD_FOLDER

RUN python3 ./setup.py install && \
    chown -R 1000:1000 /var/opt/millegrilles

VOLUME ["/var/opt/millegrilles/staging"]

WORKDIR /opt/millegrilles/dist

USER 1000:1000

CMD ["-m", "millegrilles_midcompte"]
