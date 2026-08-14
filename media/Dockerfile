FROM registry.millegrilles.com/millegrilles/messages_python:2026.4.13 AS stage1

ENV BUILD_FOLDER=/opt/millegrilles/build \
    BUNDLE_FOLDER=/opt/millegrilles/dist \
    PYTHONPATH=/opt/millegrilles/dist \
    SRC_FOLDER=/opt/millegrilles/build/src

COPY requirements.txt $BUILD_FOLDER/requirements.txt

WORKDIR /opt/millegrilles/build

RUN apt-get update && \
    apt-get install -y libmagickwand-dev ghostscript ffmpeg && \
    apt-get clean

RUN ls -R $BUILD_FOLDER && \
    pip3 install --no-cache-dir -r $BUILD_FOLDER/requirements.txt

# Reference : https://stackoverflow.com/questions/52998331/imagemagick-security-policy-pdf-blocking-conversion#53180170
#RUN cp /etc/ImageMagick-6/policy.xml /etc/ImageMagick-6/policy.xml.old && \
#    sed -i -e 's/rights="none" pattern="PDF"/rights="read" pattern="PDF"/' /etc/ImageMagick-6/policy.xml

# Final stage
FROM stage1

ENV CA_PATH=/run/secrets/millegrille.cert.pem \
    KEY_PATH=/run/secrets/key_cert.pem \
    MQ_HOSTNAME=mq \
    MQ_PORT=5673 \
    REDIS_HOSTNAME=redis \
    REDIS_PASSWORD_PATH=/run/secrets/passwd.redis.txt \
    REDIS_PORT=6379

COPY build.txt .
COPY ./millegrilles_media $BUILD_FOLDER/millegrilles_media

RUN python3 ./setup.py install && \
    mkdir -p /var/opt/millegrilles/staging && \
    chown 1000:1000 /var/opt/millegrilles/staging

USER 1000:1000

VOLUME ["/var/opt/millegrilles/staging"]

WORKDIR /opt/millegrilles/dist

CMD ["-m", "millegrilles_media"]
