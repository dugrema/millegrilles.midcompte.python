import aiohttp
import asyncio
import tempfile

from typing import Optional

from millegrilles_messages.chiffrage.Mgs4 import CipherMgs4WithSecret

BATCH_UPLOAD_DEFAULT = 100_000_000
CHUNK_SIZE = 64 * 1024


class EtatUpload:

    def __init__(self, entretien_db):
        pass


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


async def feed_filepart(input_stream, stream, limit=BATCH_UPLOAD_DEFAULT):
    taille_uploade = 0
    done = False
    transport = TransportStream()
    stream.set_transport(transport)

    while taille_uploade < limit:
        while not transport.is_reading():
            if transport.closed:
                raise Exception('stream closed')
            await asyncio.sleep(0.1)

        chunk = input_stream.read(CHUNK_SIZE)
        stream.feed_data(chunk)
        taille_uploade += len(chunk)

        if not chunk:
            done = True
            break

        await asyncio.sleep(0.001)  # Yield

    stream.feed_eof()
    return taille_uploade, done


async def uploader_fichier(
        fp_file,
        session: aiohttp.ClientSession,
        etat_fichiers,
        fuuid,
        start_pos: Optional[int] = 0,
        batch_size=BATCH_UPLOAD_DEFAULT):
    ssl_context = etat_fichiers.ssl_context
    url_fichier = f'{etat_fichiers.url_consignation_primaire}/fichiers_transfert/{fuuid}'

    headers = {'x-fuuid': fuuid}

    done = False
    position = start_pos or 0
    while not done:
        # Creer stream-reader pour lire chunks d'un fichier
        stream = asyncio.StreamReader()
        session_coro = session.put(f'{url_fichier}/{position}', ssl=ssl_context, headers=headers, data=stream)
        stream_coro = feed_filepart(fp_file, stream, limit=batch_size)

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
