FROM docker.maceroc.com/millegrilles_messages_python:2022.3.0

ENV BUILD_FOLDER=/opt/millegrilles/build \
    BUNDLE_FOLDER=/opt/millegrilles/dist \
    PYTHONPATH=/opt/millegrilles/dist \
    SRC_FOLDER=/opt/millegrilles/build/src

COPY . $BUILD_FOLDER

WORKDIR /opt/millegrilles/dist
ENTRYPOINT ["python3"]

RUN pip3 install --no-cache-dir -r $BUILD_FOLDER/requirements.txt && \
    cd $BUILD_FOLDER\
    python3 ./setup.py install && \
    cd / && \
    rm -rf $BUILD_FOLDER
