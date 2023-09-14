import datetime

import aiohttp
import asyncio
import tempfile

from typing import Optional

from millegrilles_messages.chiffrage.Mgs4 import CipherMgs4WithSecret

BATCH_UPLOAD_DEFAULT = 100_000_000
CHUNK_SIZE = 64 * 1024
CONST_LIMITE_SAMPLES_UPLOAD = 50


class EtatUpload:

    def __init__(self, fuuid: str, fp_file, stop_event: asyncio.Event, taille):
        self.fuuid = fuuid
        self.fp_file = fp_file
        self.stop_event = stop_event
        self.taille = taille
        self.position = 0
        self.samples = list()
        self.cb_activite = None


class TransportStream(asyncio.ReadTransport):

    def __init__(self):
        super().__init__()
        self.reading = True
        self.closed = False

    def close(self) -> None:
        self.closed = True

    def is_reading(self) -> bool:
        return self.reading

    def pause_reading(self) -> None:
        self.reading = False

    def resume_reading(self) -> None:
        self.reading = True


async def feed_filepart(etat_upload: EtatUpload, stream, limit=BATCH_UPLOAD_DEFAULT):
    taille_uploade = 0
    done = False
    transport = TransportStream()
    stream.set_transport(transport)

    stop_coro = asyncio.create_task(etat_upload.stop_event.wait())

    input_stream = etat_upload.fp_file

    debut_chunk = datetime.datetime.now()
    while taille_uploade < limit:
        if etat_upload.stop_event.is_set():
            return  # Stopped

        # Calcule temps transfert chunk
        now = datetime.datetime.utcnow()

        while not transport.is_reading():
            if transport.closed:
                raise Exception('stream closed')
            await asyncio.wait([stop_coro], timeout=0.01)  # Yield, attente ouverture
            if etat_upload.stop_event.is_set():
                return  # Stop

        chunk = input_stream.read(CHUNK_SIZE)

        if not chunk:
            done = True
            break

        stream.feed_data(chunk)
        taille_uploade += len(chunk)
        etat_upload.position += len(chunk)

        duree_transfert = now - debut_chunk
        etat_upload.samples.append({'duree': duree_transfert, 'taille': len(chunk)})
        while len(etat_upload.samples) > CONST_LIMITE_SAMPLES_UPLOAD:
            etat_upload.samples.pop(0)  # Detruire vieux samples

        if etat_upload.cb_activite:
            await etat_upload.cb_activite()

        debut_chunk = now
        await asyncio.wait([stop_coro], timeout=0.0001)  # Yield

    stop_coro.cancel()
    await asyncio.wait([stop_coro], timeout=1)  # Cancel

    stream.feed_eof()
    return taille_uploade, done


async def uploader_fichier(
        session: aiohttp.ClientSession,
        etat_fichiers,
        etat_upload: EtatUpload,
        batch_size=BATCH_UPLOAD_DEFAULT):

    fuuid = etat_upload.fuuid

    ssl_context = etat_fichiers.ssl_context
    url_fichier = f'{etat_fichiers.url_consignation_primaire}/fichiers_transfert/{fuuid}'

    headers = {'x-fuuid': fuuid}

    done = False
    position = etat_upload.position
    while not done:
        # Creer stream-reader pour lire chunks d'un fichier
        stream = asyncio.StreamReader()
        session_coro = session.put(f'{url_fichier}/{position}', ssl=ssl_context, headers=headers, data=stream)
        stream_coro = feed_filepart(etat_upload, stream, limit=batch_size)

        # Uploader chunk
        session_response = None
        try:
            session_response, stream_response = await asyncio.gather(session_coro, stream_coro)
        finally:
            if session_response is not None:
                session_response.release()
            session_response.raise_for_status()

        # Incrementer position pour prochain chunk
        position += stream_response[0]
        done = stream_response[1]

    async with session.post(url_fichier, ssl=ssl_context, headers=headers) as resp:
        resp.raise_for_status()
