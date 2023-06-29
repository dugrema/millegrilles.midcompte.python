import asyncio
import tempfile
import multibase

from typing import Optional

from wand.image import Image

from millegrilles_messages.messages.CleCertificat import CleCertificat
from millegrilles_messages.chiffrage.Mgs4 import CipherMgs4WithSecret


async def traiter_image(tmp_file, clecert: CleCertificat, cle):
    """
    Converti une image en jpg thumbnail, small et webp large
    :param tmp_file:
    :return:
    """
    tmp_file.seek(0)  # Rewind pour traitement
    loop = asyncio.get_running_loop()

    cle_bytes = clecert.dechiffrage_asymmetrique(cle['cle'])

    with tempfile.TemporaryFile() as tmp_output_large:
        with tempfile.TemporaryFile() as tmp_output_small:
            with Image(file=tmp_file) as original:
                if original.alpha_channel:
                    original.alpha_channel = False

                with await loop.run_in_executor(None, original.convert, 'jpeg') as img:
                    conversions = [
                        loop.run_in_executor(None, convertir_thumbnail, img.clone(), cle_bytes),
                        loop.run_in_executor(None, convertir_small, img.clone(), tmp_output_small, cle_bytes),
                        loop.run_in_executor(None, convertir_large, original, tmp_output_large, cle_bytes),
                    ]
                    thumbnail, small, large = await asyncio.gather(*conversions)

            # Debug, conserver images dans /tmp
            # with open('/home/mathieu/tmp/a_thumb.jpg', 'wb') as fichier:
            #     fichier.write(thumbnail)
            # with open('/home/mathieu/tmp/a_small.jpg', 'wb') as fichier:
            #     fichier.write(tmp_output_small.read())
            # with open('/home/mathieu/tmp/a_large.webp', 'wb') as fichier:
            #     fichier.write(tmp_output_large.read())

    pass


def convertir_thumbnail(img: Image, cle_bytes: bytes) -> dict:
    img.compression_quality = 25
    img.thumbnail(128, 128)
    return chiffrer_image(img, cle_bytes)


def convertir_small(img: Image, tmp_out: tempfile.TemporaryFile, cle_bytes: bytes) -> dict:
    img.thumbnail(200, 200)
    return chiffrer_image(img, cle_bytes, tmp_out)


def convertir_large(img, tmp_out, cle_bytes: bytes):
    width, height = img.size
    ratio_inverse = width < height
    operation_resize = '>'

    if ratio_inverse:
        valRef = width
    else:
        valRef = height
    valAutre = round(valRef * (16 / 9))
    if ratio_inverse:
        geometrie = '%dx%d%s' % (valRef, valAutre, operation_resize)
    else:
        geometrie = '%dx%d%s' % (valAutre, valRef, operation_resize)

    img.transform(resize=geometrie)
    tmp_out.seek(0)
    with img.convert('webp') as converted:
        info_fichier = chiffrer_image(converted, cle_bytes, tmp_out, mimetype='image/webp')

    tmp_out.seek(0)
    return info_fichier


def chiffrer_image(img: Image, cle_bytes: bytes, tmp_out: Optional[tempfile.TemporaryFile] = None, mimetype='image/jpeg') -> Optional[dict]:
    jpeg_bin = img.make_blob()

    # Chiffrer bytes
    cipher = CipherMgs4WithSecret(cle_bytes)
    jpeg_bin = cipher.update(jpeg_bin)
    jpeg_bin += cipher.finalize()

    resolution = min(*img.size)

    info_fichier = {
        'hachage': cipher.hachage,
        'width': img.size[0],
        'height': img.size[1],
        'mimetype': mimetype,
        'taille': len(jpeg_bin),
        'resolution': resolution,
        'header': multibase.encode('base64', cipher.header).decode('utf-8'),
        'format': 'mgs4',
    }

    if tmp_out is not None:
        # Sauvegarder dans fichier tmp
        tmp_out.seek(0)
        tmp_out.write(jpeg_bin)
        tmp_out.seek(0)
    else:
        info_fichier['data_chiffre'] = multibase.encode('base64', jpeg_bin).decode('utf-8')

    return info_fichier


async def traiter_poster_video(tmp_file, cle_bytes: bytes):
    """
    Genere un thumbnail/small jpg et poster webp
    :param tmp_file:
    :return:
    """
    pass
