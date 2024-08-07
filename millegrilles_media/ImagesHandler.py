import aiohttp
import asyncio
import tempfile
import multibase

from typing import Optional

import ffmpeg
from wand.image import Image
from wand.color import Color

from millegrilles_messages.chiffrage.Mgs4 import CipherMgs4WithSecret
from millegrilles_media.EtatMedia import EtatMedia
from millegrilles_media.TransfertFichiers import uploader_fichier


async def traiter_image(job, tmp_file, etat_media: EtatMedia, info_video: Optional[dict] = None):
    """
    Converti une image en jpg thumbnail, small et webp large
    :param tmp_file:
    :param etat_media:
    :param cle:
    :param info_video:
    :return:
    """
    loop = asyncio.get_running_loop()

    # clecert = etat_media.clecertificat
    info_dechiffrage = job['cle']
    cle_bytes: bytes = multibase.decode('m' + info_dechiffrage['cle_secrete_base64'])
    cle_id = info_dechiffrage['cle_id']
    # cle_bytes = clecert.dechiffrage_asymmetrique(cle['cle'])

    dir_staging = etat_media.configuration.dir_staging

    with tempfile.TemporaryFile(dir=dir_staging) as tmp_output_large:
        with tempfile.TemporaryFile(dir=dir_staging) as tmp_output_small:
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
                        loop.run_in_executor(None, convertir_thumbnail, img.clone(), cle_bytes, cle_id),
                        loop.run_in_executor(None, convertir_small, img.clone(), tmp_output_small, cle_bytes, cle_id),
                        loop.run_in_executor(None, convertir_large, original, tmp_output_large, cle_bytes, cle_id),
                    ]
                    thumbnail, small, large = await asyncio.gather(*conversions)

            await uploader_images(etat_media, job, info_original, thumbnail, small, large, tmp_output_small, tmp_output_large, info_video)


def convertir_thumbnail(img: Image, cle_bytes: bytes, cle_id: str) -> dict:
    taille = min(*img.size)
    img.compression_quality = 25
    img.crop(width=taille, height=taille, gravity='center')
    img.resize(128, 128)
    img.strip()
    return chiffrer_image(img, cle_bytes, cle_id)


def convertir_small(img: Image, tmp_out: tempfile.TemporaryFile, cle_bytes: bytes, cle_id: str) -> dict:
    taille = min(*img.size)
    img.crop(width=taille, height=taille, gravity='center')
    img.resize(200, 200)
    img.strip()
    return chiffrer_image(img, cle_bytes, cle_id, tmp_out)


def convertir_large(img, tmp_out, cle_bytes: bytes, cle_id: str):
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
        info_fichier = chiffrer_image(converted, cle_bytes, cle_id, tmp_out, mimetype='image/webp')

    tmp_out.seek(0)
    return info_fichier


def chiffrer_image(img: Image, cle_bytes: bytes, cle_id: str, tmp_out: Optional[tempfile.TemporaryFile] = None, mimetype='image/jpeg') -> Optional[dict]:
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
        # 'header': multibase.encode('base64', cipher.header).decode('utf-8'),
        'nonce': multibase.encode('base64', cipher.header).decode('utf-8')[1:],  # Retirer 'm' multibase
        'format': 'mgs4',
        'cle_id': cle_id,
    }

    if tmp_out is not None:
        # Sauvegarder dans fichier tmp
        tmp_out.seek(0)
        tmp_out.write(jpeg_bin)
        tmp_out.seek(0)
    else:
        info_fichier['data_chiffre'] = multibase.encode('base64', jpeg_bin).decode('utf-8')

    return info_fichier


async def traiter_poster_video(job, tmp_file_video: tempfile.TemporaryFile, etat_media: EtatMedia):
    """
    Genere un thumbnail/small jpg et poster webp
    :param tmp_file_video:
    :param etat_media:
    :param cle:
    :return:
    """
    loop = asyncio.get_running_loop()

    # Extraire un snapshot de reference du video
    dir_staging = etat_media.configuration.dir_staging
    tmp_file_snapshot = tempfile.NamedTemporaryFile(dir=dir_staging, suffix='.jpg')
    try:
        probe = ffmpeg.probe(tmp_file_video.name)
        try:
            duration = float(probe['format']['duration'])
            snapshot_position = duration * 0.2
        except KeyError:
            snapshot_position = 5  # Mettre a 5 secondes, duree non disponible
        stream = ffmpeg \
            .input(tmp_file_video.name, ss=snapshot_position) \
            .output(tmp_file_snapshot.name, vframes=1) \
            .overwrite_output()

        await loop.run_in_executor(None, stream.run)

        # Fermer/supprimer fichier original (dechiffre)
        tmp_file_video.close()

        # Traiter et uploader le snapshot
        await traiter_image(job, tmp_file_snapshot, etat_media, info_video=probe)
    finally:
        if tmp_file_snapshot.closed is False:
            tmp_file_snapshot.close()


async def uploader_images(
        etat_media: EtatMedia, job: dict, info_original: dict, thumbnail, small, large,
        tmpfile_small: tempfile.TemporaryFile, tmpfile_large: tempfile.TemporaryFile,
        info_video: Optional[dict] = None):

    commande_associer = preparer_commande_associer(job, info_original, thumbnail, small, large, info_video)

    # Uploader les fichiers temporaires
    timeout = aiohttp.ClientTimeout(connect=5, total=240)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        await uploader_fichier(session, etat_media, large['hachage'], tmpfile_large)
        await uploader_fichier(session, etat_media, small['hachage'], tmpfile_small)

    # Transmettre commande associer
    producer = etat_media.producer
    await producer.executer_commande(commande_associer,
                                     domaine='GrosFichiers', action='associerConversions', exchange='2.prive')


def preparer_commande_associer(
        job: dict, info_original: dict, thumbnail, small, large,
        info_video: Optional[dict] = None) -> dict:

    mimetype = job['mimetype']

    label_large = '%s;%s' % (large['mimetype'], large['resolution'])

    anime = info_video is not None or (mimetype.startswith('image/') and info_original['frames'] > 1)

    images = {
        'thumb': thumbnail,
        'small': small,
        label_large: large,
    }

    commande_associer = {
        'tuuid': job['tuuid'],
        'fuuid': job['fuuid'],
        'user_id': job['user_id'],
        'width': info_original['width'],
        'height': info_original['height'],
        'mimetype': info_original['mimetype'],
        'images': images
    }

    if anime:
        commande_associer['anime'] = True

    if info_video is not None:
        video_stream = next([s for s in info_video['streams'] if s['codec_type'] == 'video'].__iter__())
        try:
            audio_stream = next([s for s in info_video['streams'] if s['codec_type'] == 'audio'].__iter__())
        except StopIteration:
            audio_stream = None
        commande_associer['mimetype'] = job['mimetype']  # Override mimetype image snapshot
        try:
            commande_associer['duration'] = float(info_video['format']['duration'])
        except KeyError:
            pass  # Duration non disponible

        if video_stream is not None:
            codec_video = video_stream['codec_name']
            commande_associer['videoCodec'] = codec_video
            try:
                nb_frames = video_stream['nb_frames']
                commande_associer['metadata'] = {'nbFrames': nb_frames}
            except KeyError:
                pass

        if audio_stream is not None:
            codec_audio = audio_stream['codec_name']
            commande_associer['audioCodec'] = codec_audio

    return commande_associer
