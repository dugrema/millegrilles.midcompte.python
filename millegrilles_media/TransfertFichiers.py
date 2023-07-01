import asyncio

import aiohttp
import tempfile

from millegrilles_messages.chiffrage.Mgs4 import CipherMgs4WithSecret

BATCH_UPLOAD_DEFAULT = 100_000_000
CHIFFRER_CHUNK_SIZE = 64 * 1024


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


async def feed_filepart(tmp_file, stream, limit=BATCH_UPLOAD_DEFAULT):
    taille_uploade = 0
    done = False
    transport = TransportStream()
    stream.set_transport(transport)

    while taille_uploade < limit:
        while not transport.is_reading():
            if transport.closed:
                raise Exception('stream closed')
            await asyncio.sleep(0.1)

        chunk = tmp_file.read(CHIFFRER_CHUNK_SIZE)
        stream.feed_data(chunk)
        taille_uploade += len(chunk)

        if not chunk:
            done = True
            break

        await asyncio.sleep(0.001)  # Yield

    stream.feed_eof()
    return taille_uploade, done


async def uploader_fichier(session: aiohttp.ClientSession, etat_media, fuuid,
                           tmp_file: tempfile.TemporaryFile,
                           batch_size=BATCH_UPLOAD_DEFAULT):
    ssl_context = etat_media.ssl_context
    url_fichier = f'{etat_media.url_consignation}/fichiers_transfert/{fuuid}'

    tmp_file.seek(0)
    headers = {'x-fuuid': fuuid}

    done = False
    position = 0
    while not done:
        # Creer stream-reader pour lire chunks d'un fichier
        stream = asyncio.StreamReader()
        session_coro = session.put(f'{url_fichier}/{position}', ssl=ssl_context, headers=headers, data=stream)
        stream_coro = feed_filepart(tmp_file, stream, limit=batch_size)

        # Uploader chunk
        session_response = None
        try:
            session_response, stream_response = await asyncio.gather(session_coro, stream_coro)
        finally:
            if session_response is not None:
                session_response.release()

        # Incrementer position pour prochain chunk
        position += stream_response[0]
        done = stream_response[1]

    async with session.post(url_fichier, ssl=ssl_context, headers=headers) as resp:
        resp.raise_for_status()


async def chiffrer_fichier(cle_bytes: bytes, src: tempfile.TemporaryFile, dest: tempfile.TemporaryFile) -> dict:
    loop = asyncio.get_running_loop()

    src.seek(0)
    cipher = CipherMgs4WithSecret(cle_bytes)
    await loop.run_in_executor(None, __chiffrer, cipher, src, dest)

    return {
        'hachage': cipher.hachage,
        'header': cipher.header,
        'taille_chiffree': cipher.taille_chiffree,
        'taille_dechiffree': cipher.taille_dechiffree,
    }


def __chiffrer(cipher, src, dest):
    while True:
        chunk = src.read(64 * 1024)
        if len(chunk) == 0:
            break
        dest.write(cipher.update(chunk))

    # Finalizer l'ecriture
    dest.write(cipher.finalize())
