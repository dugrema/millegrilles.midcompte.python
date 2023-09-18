import datetime
import logging

import aiohttp
import asyncio
import tempfile

from typing import Optional

from millegrilles_messages.chiffrage.Mgs4 import CipherMgs4WithSecret

BATCH_UPLOAD_DEFAULT = 100_000_000
CHUNK_SIZE = 64 * 1024
CONST_LIMITE_SAMPLES_UPLOAD = 50

LOGGER = logging.getLogger('millegrilles_fichiers.UploadFichiersPrimaire')


class EtatUpload:

    def __init__(self, fuuid: str, fp_file, stop_event: asyncio.Event, taille):
        self.fuuid = fuuid
        self.fp_file = fp_file
        self.stop_event = stop_event
        self.taille = taille
        self.position = 0
        self.samples = list()
        self.cb_activite = None
        self.done = False


async def feed_filepart2(etat_upload: EtatUpload, limit=BATCH_UPLOAD_DEFAULT):
    taille_uploade = 0
    input_stream = etat_upload.fp_file

    debut_chunk = datetime.datetime.now()
    while taille_uploade < limit:
        if etat_upload.stop_event.is_set():
            break  # Stopped

        chunk = input_stream.read(CHUNK_SIZE)
        if not chunk:
            etat_upload.done = True
            break

        yield chunk

        taille_uploade += len(chunk)
        etat_upload.position += len(chunk)

        if etat_upload.cb_activite:
            await etat_upload.cb_activite()

        # Calcule temps transfert chunk
        now = datetime.datetime.utcnow()
        duree_transfert = now - debut_chunk
        etat_upload.samples.append({'duree': duree_transfert, 'taille': len(chunk)})
        while len(etat_upload.samples) > CONST_LIMITE_SAMPLES_UPLOAD:
            etat_upload.samples.pop(0)  # Detruire vieux samples

        debut_chunk = now


async def uploader_fichier(
        session: aiohttp.ClientSession,
        etat_fichiers,
        event_done: asyncio.Event,
        etat_upload: EtatUpload,
        batch_size=BATCH_UPLOAD_DEFAULT):

    fuuid = etat_upload.fuuid

    ssl_context = etat_fichiers.ssl_context
    url_fichier = f'{etat_fichiers.url_consignation_primaire}/fichiers_transfert/{fuuid}'

    headers = {'x-fuuid': fuuid}

    try:
        while not etat_upload.done:
            position = etat_upload.position
            feeder_coro = feed_filepart2(etat_upload, batch_size)
            session_coro = session.put(f'{url_fichier}/{position}', ssl=ssl_context, headers=headers, data=feeder_coro)

            # Uploader chunk
            session_response = None
            try:
                session_response = await session_coro
            finally:
                if session_response is not None:
                    session_response.release()
                    session_response.raise_for_status()

        async with session.post(url_fichier, ssl=ssl_context, headers=headers) as resp:
            resp.raise_for_status()
    finally:
        event_done.set()

