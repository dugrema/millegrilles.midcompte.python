import tempfile

from wand.image import Image


async def traiter_image(tmp_file):
    """
    Converti une image en jpg thumbnail, small et webp large
    :param tmp_file:
    :return:
    """
    tmp_file.seek(0)  # Rewind pour traitement

    with tempfile.TemporaryFile() as tmp_output_large:
        with tempfile.TemporaryFile() as tmp_output_small:
            with Image(file=tmp_file) as original:
                if original.alpha_channel:
                    original.alpha_channel = False

                with original.convert('jpeg') as img:
                    thumbnail = await convertir_thumbnail(img.clone())
                    await convertir_small(img.clone(), tmp_output_small)
                    await convertir_large(img, tmp_output_large)

            # Debug, conserver images dans /tmp
            with open('/tmp/a_thumb.jpg', 'wb') as fichier:
                fichier.write(thumbnail)
            with open('/tmp/a_small.jpg', 'wb') as fichier:
                fichier.write(tmp_output_small.read())
            with open('/tmp/a_large.webp', 'wb') as fichier:
                fichier.write(tmp_output_large.read())

    pass


async def convertir_thumbnail(img) -> bytes:
    img.compression_quality = 25
    img.thumbnail(128, 128)
    jpeg_bin = img.make_blob()
    return jpeg_bin


async def convertir_small(img, tmp_out):
    tmp_out.seek(0)
    img.thumbnail(200, 200)
    img.save(file=tmp_out)
    tmp_out.seek(0)


async def convertir_large(img, tmp_out):
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

    tmp_out.seek(0)
    img.transform(resize=geometrie)
    with img.convert('webp') as converted:
        converted.save(file=tmp_out)
    tmp_out.seek(0)


async def traiter_poster_video(tmp_file):
    """
    Genere un thumbnail/small jpg et poster webp
    :param tmp_file:
    :return:
    """
    pass
