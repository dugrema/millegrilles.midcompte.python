import aiohttp
import tempfile

from millegrilles_messages.chiffrage.Mgs4 import CipherMgs4WithSecret

BATCH_UPLOAD_DEFAULT = 100_000_000


async def uploader_fichier(session: aiohttp.ClientSession, etat_media, fuuid,
                           tmp_file: tempfile.TemporaryFile,
                           batch_size=BATCH_UPLOAD_DEFAULT):
    ssl_context = etat_media.ssl_context
    url_fichier = f'{etat_media.url_consignation}/fichiers_transfert/{fuuid}'

    tmp_file.seek(0)
    headers = {'x-fuuid': fuuid}
    async with session.put(f'{url_fichier}/0', ssl=ssl_context, headers=headers, data=tmp_file) as resp:
        resp.raise_for_status()

    async with session.post(url_fichier, ssl=ssl_context, headers=headers) as resp:
        resp.raise_for_status()


async def chiffrer_fichier(cle_bytes: bytes, src: tempfile.TemporaryFile, dest: tempfile.TemporaryFile) -> dict:
    cipher = CipherMgs4WithSecret(cle_bytes)
    async for chunk in src.iter_chunked(64 * 1024):
        dest.write(cipher.update(chunk))
    dest.write(cipher.finalize())

    return {
        'hachage': cipher.hachage,
        'header': cipher.header,
        'taille_chiffree': cipher.taille_chiffree,
        'taille_dechiffree': cipher.taille_dechiffree,
    }
