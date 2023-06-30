import aiohttp
import tempfile

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

