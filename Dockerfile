FROM docker.maceroc.com/millegrilles_messages_python:2022.4.0

ENV BUILD_FOLDER=/opt/millegrilles/build \
    BUNDLE_FOLDER=/opt/millegrilles/dist \
    PYTHONPATH=/opt/millegrilles/dist \
    SRC_FOLDER=/opt/millegrilles/build/src

COPY . $BUILD_FOLDER

WORKDIR /opt/millegrilles/build

RUN pip3 install --no-cache-dir -r $BUILD_FOLDER/requirements.txt && \
    python3 ./setup.py install

WORKDIR /opt/millegrilles/dist

CMD ["-m", "millegrilles_midcompte", "--verbose"]
