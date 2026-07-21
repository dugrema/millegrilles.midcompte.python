import asyncio
from urllib.parse import urljoin

import aiohttp
import tempfile

from millegrilles_media.Context import MediaContext
from millegrilles_messages.messages import Constantes
from millegrilles_messages.chiffrage.Mgs4 import CipherMgs4WithSecret

BATCH_UPLOAD_DEFAULT = 1_000_000_000
CHIFFRER_CHUNK_SIZE = 64 * 1024


class EtatUpload:

    def __init__(self, fuuid: str, fp_file, stop_event: asyncio.Event, taille: int):
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


async def uploader_fichier(session: aiohttp.ClientSession, context: MediaContext, fuuid, file_size: int,
                           tmp_file: tempfile.TemporaryFile, batch_size=BATCH_UPLOAD_DEFAULT):
    # url_fichier = f'{etat_media.url_}filehost/files/{fuuid}'
    filehost_url = context.filehost_url
    url_fichier = urljoin(filehost_url, f'/filehost/files/{fuuid}')

    tmp_file.seek(0)
    headers = {'x-fuuid': fuuid}

    stop_event = asyncio.Event()
    etat_upload = EtatUpload(fuuid, tmp_file, stop_event, 0)
    one_shot_upload = file_size <= batch_size

    while not etat_upload.done:
        position = etat_upload.position
        if one_shot_upload:
            headers['Content-Length'] = str(file_size)
            upload_url = f'{url_fichier}'
        else:
            upload_url = f'{url_fichier}/{position}'

        feeder_coro = feed_filepart2(etat_upload, limit=batch_size)
        session_coro = session.put(upload_url, headers=headers, data=feeder_coro)

        # Uploader chunk
        session_response = None
        try:
            session_response = await session_coro
            session_response.raise_for_status()
        finally:
            if session_response is not None:
                session_response.release()

    if one_shot_upload is False:
        async with session.post(url_fichier) as resp:
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


async def filehost_authenticate(context: MediaContext, session: aiohttp.ClientSession):
    filehost_url = context.filehost_url
    url_authenticate = urljoin(filehost_url, '/filehost/authenticate')
    authentication_message, message_id = context.formatteur.signer_message(
        Constantes.KIND_COMMANDE, dict(), domaine='filehost', action='authenticate')
    authentication_message['millegrille'] = context.formatteur.enveloppe_ca.certificat_pem
    async with session.post(url_authenticate, json=authentication_message) as resp:
        resp.raise_for_status()

