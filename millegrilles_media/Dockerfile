FROM docker.maple.maceroc.com:5000/millegrilles_messages_python:2024.9.81

ENV BUILD_FOLDER=/opt/millegrilles/build \
    BUNDLE_FOLDER=/opt/millegrilles/dist \
    PYTHONPATH=/opt/millegrilles/dist \
    SRC_FOLDER=/opt/millegrilles/build/src \
    CA_PATH=/run/secrets/millegrille.cert.pem \
    CERT_PATH=/run/secrets/cert.pem \
    KEY_PATH=/run/secrets/key.pem \
    MQ_HOSTNAME=mq \
    MQ_PORT=5673 \
    REDIS_HOSTNAME=redis \
    REDIS_PASSWORD_PATH=/run/secrets/passwd.redis.txt \
    REDIS_PORT=6379

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

CMD ["-m", "millegrilles_media"]
