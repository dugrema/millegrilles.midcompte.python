import aiohttp
import asyncio
import datetime
import tempfile

from typing import Optional

import ffmpeg
import multibase

from millegrilles_media.EtatMedia import EtatMedia
from millegrilles_media.TransfertFichiers import uploader_fichier, chiffrer_fichier


async def traiter_video(etat_media: EtatMedia, job: dict, tmp_file: tempfile.NamedTemporaryFile()):
    """
    Converti une image en jpg thumbnail, small et webp large
    :param etat_media:
    :param job:
    :param tmp_file:
    :return:
    """
    loop = asyncio.get_running_loop()

    tmp_transcode = tempfile.NamedTemporaryFile()
    try:
        # Convertir le video

        # conversions = [
        #     loop.run_in_executor(None, convertir_thumbnail, img.clone(), cle_bytes),
        #     loop.run_in_executor(None, convertir_small, img.clone(), tmp_output_small, cle_bytes),
        #     loop.run_in_executor(None, convertir_large, original, tmp_output_large, cle_bytes),
        # ]
        # thumbnail, small, large = await asyncio.gather(*conversions)

        # Fermer le fichier original (supprime le tmp file)
        tmp_file.close()

        with tempfile.TemporaryFile() as tmp_output_chiffre:
            # Chiffrer output
            info_chiffrage = await chiffrer_video(etat_media, job, tmp_transcode, tmp_output_chiffre)

            # Fermer fichier dechiffre
            tmp_transcode.close()
            tmp_transcode = None

            # Uploader fichier chiffre
            tmp_output_chiffre.seek(0)
            await uploader_video(etat_media, job, info_chiffrage, tmp_output_chiffre)
    finally:
        if tmp_transcode is not None:
            tmp_transcode.close()


def chiffrer_video(etat_media, job: dict, tmp_input: tempfile.TemporaryFile, tmp_output: tempfile.TemporaryFile) -> Optional[dict]:
    info_video = probe_video(tmp_output.name)

    clecert = etat_media.clecertificat
    cle = job['cle']
    cle_bytes = clecert.dechiffrage_asymmetrique(cle['cle'])

    tmp_input.seek(0)
    info_chiffrage = await chiffrer_fichier(cle_bytes, tmp_input, tmp_output)

    mimetype = 'video/%s' % job['format']

    info_fichier = {
        'hachage': info_chiffrage['hachage'],
        'mimetype': mimetype,
        'taille_fichier': info_chiffrage['taille_chiffree'],
        'taille_originale': info_chiffrage['taille_dechiffree'],
        'header': multibase.encode('base64', info_chiffrage['header']).decode('utf-8'),
        'format': 'mgs4',
    }

    info_video.update(info_fichier)

    return info_video


def probe_video(filepath) -> dict:
    info_probe = ffmpeg.probe(filepath)

    info_video = dict()

    video_stream = next((stream for stream in info_probe['streams'] if stream['codec_type'] == 'video'), None)
    audio_stream = next([s for s in info_probe['streams'] if s['codec_type'] == 'audio'].__iter__())
    info_video['duration'] = float(info_probe['format']['duration'])

    if video_stream is not None:
        codec_video = video_stream['codec_name']
        nb_frames = video_stream['nb_frames']
        info_video['videoCodec'] = codec_video
        info_video['metadata'] = {'nbFrames': nb_frames}

    if audio_stream is not None:
        codec_audio = audio_stream['codec_name']
        info_video['audioCodec'] = codec_audio

    width = int(video_stream['width'])
    height = int(video_stream['height'])
    info_video['width'] = width
    info_video['height'] = height
    info_video['frames'] = int(video_stream['nb_frames'])
    info_video['resolution'] = min(width, height)

    return info_video


def run_stream(process):
    # stream.run(capture_stdout=True, capture_stderr=True)
    out, err = process.communicate(None)
    retcode = process.poll()
    if retcode:
        raise ffmpeg.Error('error', out, err)
    return out, err


def progress_handler(etat, probe_info, key, value):
    # print("%s = %s" % (key, value))
    if key == 'frame':
        now = datetime.datetime.now().timestamp()
        if etat['dernier_update'] + etat['intervalle_update'] < now:
            progres = round(float(value) / etat['frames'] * 100)
            print(f'Progres : {progres}%')
            etat['dernier_update'] = now


def _do_watch_progress(probe_info, sock, handler):
    """Function to run in a separate gevent greenlet to read progress
    events from a unix-domain socket."""
    connection, client_address = sock.accept()
    data = b''

    video_stream = next((stream for stream in probe_info['streams'] if stream['codec_type'] == 'video'), None)
    etat = {
        'dernier_update': 0,
        'intervalle_update': 3,
        'frames': float(video_stream['nb_frames']),
    }
    try:
        while True:
            more_data = connection.recv(16)
            if not more_data:
                break
            data += more_data
            lines = data.split(b'\n')
            for line in lines[:-1]:
                line = line.decode()
                parts = line.split('=')
                key = parts[0] if len(parts) > 0 else None
                value = parts[1] if len(parts) > 1 else None
                handler(etat, probe_info, key, value)
            data = lines[-1]
    finally:
        connection.close()


async def uploader_video(etat_media, job, info_chiffrage, tmp_output_chiffre):
    commande_associer = preparer_commande_associer(job, info_chiffrage)

    # Uploader les fichiers temporaires
    tmp_output_chiffre.seek(0)
    timeout = aiohttp.ClientTimeout(connect=5, total=600)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        await uploader_fichier(session, etat_media, commande_associer['fuuid_video'], tmp_output_chiffre)

    # Transmettre commande associer
    producer = etat_media.producer
    await producer.executer_commande(commande_associer,
                                     domaine='GrosFichiers', action='associerVideo', exchange='2.prive')


def preparer_commande_associer(job: dict, info_chiffrage: dict) -> dict:
    commande = {
        "tuuid": job['tuuid'],
        "fuuid": job['fuuid'],
        "user_id": job['user_id'],
        "quality": job['quality'],

        "codec": info_chiffrage['codecVideo'],
        "codec_audio": info_chiffrage['codecAudio'],
        "fuuid_video": info_chiffrage['hachage'],
        "mimetype": info_chiffrage['mimetype'],
        "height": info_chiffrage['height'],
        "width": info_chiffrage['width'],

        "taille_fichier": info_chiffrage['taille_fichier'],
        "hachage": info_chiffrage['hachage'],
        "header": info_chiffrage['header'],
        "format": info_chiffrage['format'],
    }

    if job.get('fallback') is True:
        commande['fallback'] = True

