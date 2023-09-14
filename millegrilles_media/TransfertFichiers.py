import asyncio

import aiohttp
import tempfile

from millegrilles_messages.chiffrage.Mgs4 import CipherMgs4WithSecret

BATCH_UPLOAD_DEFAULT = 100_000_000
CHIFFRER_CHUNK_SIZE = 64 * 1024


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
    stop_coro = asyncio.create_task(etat_upload.stop_event.wait())

    input_stream = etat_upload.fp_file

    while taille_uploade < limit:
        if etat_upload.stop_event.is_set():
            break  # Stopped

        chunk = input_stream.read(CHIFFRER_CHUNK_SIZE)
        if not chunk:
            etat_upload.done = True
            break

        yield chunk

        taille_uploade += len(chunk)
        etat_upload.position += len(chunk)

        if etat_upload.cb_activite:
            await etat_upload.cb_activite()

    stop_coro.cancel()
    await asyncio.wait([stop_coro], timeout=1)  # Cancel


async def uploader_fichier(session: aiohttp.ClientSession, etat_media, fuuid,
                           tmp_file: tempfile.TemporaryFile,
                           batch_size=BATCH_UPLOAD_DEFAULT):
    ssl_context = etat_media.ssl_context
    url_fichier = f'{etat_media.url_consignation}/fichiers_transfert/{fuuid}'

    tmp_file.seek(0)
    headers = {'x-fuuid': fuuid}

    stop_event = asyncio.Event()
    etat_upload = EtatUpload(fuuid, tmp_file, stop_event, 0)
    while not etat_upload.done:
        position = etat_upload.position
        feeder_coro = feed_filepart2(etat_upload, limit=batch_size)
        session_coro = session.put(f'{url_fichier}/{position}', ssl=ssl_context, headers=headers, data=feeder_coro)

        # Uploader chunk
        session_response = None
        try:
            session_response = await session_coro
        finally:
            if session_response is not None:
                session_response.release()

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
