import asyncio
import tempfile

from typing import Optional

from wand.image import Image

from millegrilles_media.EtatMedia import EtatMedia
from millegrilles_media.TransfertFichiers import uploader_fichier


async def traiter_video(job, tmp_file, etat_media: EtatMedia, info_video: Optional[dict] = None):
    """
    Converti une image en jpg thumbnail, small et webp large
    :param tmp_file:
    :param etat_media:
    :param cle:
    :param info_video:
    :return:
    """
    loop = asyncio.get_running_loop()

    clecert = etat_media.clecertificat
    cle = job['cle']
    cle_bytes = clecert.dechiffrage_asymmetrique(cle['cle'])

    with tempfile.TemporaryFile() as tmp_output_large:
        with tempfile.TemporaryFile() as tmp_output_small:
            tmp_file.seek(0)  # Rewind pour traitement
            with Image(file=tmp_file) as original:
                tmp_file.close()  # Liberer fichier (supprime le fichier temporaire)

                frames = len(original.sequence)
                for i in range(frames - 1, 0, -1):
                    original.sequence.pop(i)

                info_original = {
                    'width': original.size[0],
                    'height': original.size[1],
                    'mimetype': original.mimetype,
                    'frames': frames,
                }

                with await loop.run_in_executor(None, original.convert, 'jpeg') as img:
                    if img.alpha_channel:
                        img.background_color = Color('white')
                        img.alpha_channel = 'remove'

                    conversions = [
                        loop.run_in_executor(None, convertir_thumbnail, img.clone(), cle_bytes),
                        loop.run_in_executor(None, convertir_small, img.clone(), tmp_output_small, cle_bytes),
                        loop.run_in_executor(None, convertir_large, original, tmp_output_large, cle_bytes),
                    ]
                    thumbnail, small, large = await asyncio.gather(*conversions)

            await uploader_images(etat_media, job, info_original, thumbnail, small, large, tmp_output_small, tmp_output_large, info_video)