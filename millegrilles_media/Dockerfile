FROM docker.maple.maceroc.com:5000/millegrilles_messages_python:2023.9.1

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

RUN apt update && \
    apt install -y libmagickwand-dev ghostscript ffmpeg && \
    pip3 install --no-cache-dir -r $BUILD_FOLDER/requirements.txt && \
    pip3 install --no-cache-dir -r $BUILD_FOLDER/millegrilles_media/requirements.txt && \
    python3 ./millegrilles_media/setup.py install && \
    apt clean && \
    mkdir -p /var/opt/millegrilles/staging && \
    chown 984:980 /var/opt/millegrilles/staging

# Reference : https://stackoverflow.com/questions/52998331/imagemagick-security-policy-pdf-blocking-conversion#53180170
RUN cp /etc/ImageMagick-6/policy.xml /etc/ImageMagick-6/policy.xml.old && \
    sed -i -e 's/rights="none" pattern="PDF"/rights="read" pattern="PDF"/' /etc/ImageMagick-6/policy.xml

# UID fichiers = 984
# GID millegrilles = 980
USER 984:980

VOLUME ["/var/opt/millegrilles/staging"]

WORKDIR /opt/millegrilles/dist

CMD ["-m", "millegrilles_media", "--verbose"]
