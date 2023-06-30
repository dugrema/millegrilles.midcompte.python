import logging

import aiohttp
import asyncio
import datetime
import os
import signal
import socket
import sys
import tempfile

from typing import Optional

import ffmpeg
import multibase

from millegrilles_media.EtatMedia import EtatMedia
from millegrilles_media.TransfertFichiers import uploader_fichier, chiffrer_fichier


LOGGER = logging.getLogger(__name__)


async def traiter_video(etat_media: EtatMedia, job: dict, tmp_file: tempfile.NamedTemporaryFile(), cancel_event: Optional[asyncio.Event] = None):
    """
    Converti une image en jpg thumbnail, small et webp large
    :param etat_media:
    :param job:
    :param tmp_file:
    :param event:
    :return:
    """
    dir_staging = etat_media.configuration.dir_staging
    tmp_transcode = tempfile.NamedTemporaryFile(dir=dir_staging)
    try:
        # Convertir le video
        params_conversion = await convertir_progress(etat_media, job, tmp_file, tmp_transcode, cancel_event)
        mimetype_output = 'video/%s' % params_conversion['format']
        job['mimetype_output'] = mimetype_output

        # Fermer le fichier original (supprime le tmp file)
        tmp_file.close()

        with tempfile.TemporaryFile(dir=dir_staging) as tmp_output_chiffre:
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


async def chiffrer_video(etat_media, job: dict, tmp_input: tempfile.NamedTemporaryFile, tmp_output: tempfile.TemporaryFile) -> Optional[dict]:
    loop = asyncio.get_running_loop()

    LOGGER.debug("probe fichier video transcode : %s" % tmp_input.name)
    tmp_input.seek(0)
    tmp_input.flush()
    info_video = await loop.run_in_executor(None, probe_video, tmp_input.name)

    clecert = etat_media.clecertificat
    cle = job['cle']
    cle_bytes = clecert.dechiffrage_asymmetrique(cle['cle'])

    tmp_input.seek(0)
    info_chiffrage = await chiffrer_fichier(cle_bytes, tmp_input, tmp_output)

    mimetype = job['mimetype_output']

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
        info_video['videoCodec'] = codec_video
        try:
            nb_frames = video_stream['nb_frames']
            info_video['metadata'] = {'nbFrames': nb_frames}
        except KeyError:
            pass

    if audio_stream is not None:
        codec_audio = audio_stream['codec_name']
        info_video['audioCodec'] = codec_audio

    width = int(video_stream['width'])
    height = int(video_stream['height'])
    info_video['width'] = width
    info_video['height'] = height
    info_video['resolution'] = min(width, height)
    try:
        info_video['frames'] = int(video_stream['nb_frames'])
    except KeyError:
        pass

    return info_video


def run_stream(process):
    # stream.run(capture_stdout=True, capture_stderr=True)
    out, err = process.communicate(None)
    retcode = process.poll()
    if retcode:
        print("Erreur ffmpeg %d : %s" % (retcode, err.decode('utf-8')), file=sys.stderr)
        raise ffmpeg.Error('error', out, err)

    LOGGER.debug("*** Output ffmpeg *** \n%s\n*******" % out)
    return out, err


def progress_handler(etat, video_info, key, value):
    # print("%s = %s" % (key, value))
    if key == 'frame':
        now = datetime.datetime.now().timestamp()
        if etat['dernier_update'] + etat['intervalle_update'] < now:
            progres = round(float(value) / etat['frames'] * 100)
            print(f'Progres : {progres}%')
            etat['dernier_update'] = now


def _do_watch_progress(video_info, sock, handler):
    """Function to run in a separate gevent greenlet to read progress
    events from a unix-domain socket."""
    connection, client_address = sock.accept()
    data = b''

    # video_stream = next((stream for stream in probe_info['streams'] if stream['codec_type'] == 'video'), None)
    etat = {
        'dernier_update': 0,
        'intervalle_update': 3,
        'frames': float(video_info['frames']),
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
                handler(etat, video_info, key, value)
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
        "quality": job['qualityVideo'],

        "codec": job['codecVideo'],
        "codec_audio": job['codecAudio'],
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

    return commande


# Params ajouter aux profils
# 'threads': 3,
# 'vf': f'scale={width_resized}:{height_resized}',
PROFIL_DEFAUT = {
    'h264': {
        'format': 'mp4',
        'vcodec': 'libx264',
        'acodec': 'aac',
        'b:a': '64k',
        'ac': 1,
        'preset': 'veryFast',
        'crf': 30,  # Quality video
    },
    'vp9': {
        'format': 'webm',
        'vcodec': 'libvpx-vp9',
        'acodec': 'libopus',
        'b:a': '128k',
        'preset': 'medium',
        'crf': 36,  # Quality video
    },
    'hevc': {
        'format': 'mp4',
        'vcodec': 'libx265',
        'acodec': 'eac3',
        'b:a': '128k',
        'preset': 'medium',
        'crf': 30,  # Quality video
    }
}

MAPPING_PARAMS = {
    'codecVideo': 'vcodec',
    'codecAudio': 'acodec',
    'bitrateAudio': 'b:a',
    'channelsAudio': 'ac',
    'preset': 'preset',
    'qualityVideo': 'crf',
}


def get_profil(job: dict) -> dict:

    codec_video = job['codecVideo']
    try:
        codec_audio = job['codecAudio']
        if codec_audio == 'opus':
            # Patch - utiliser libopus
            codec_audio = 'libopus'
    except KeyError:
        codec_audio = None

    profil = PROFIL_DEFAUT[codec_video].copy()

    for key, mapping in MAPPING_PARAMS.items():
        try:
            profil[mapping] = job[key]
        except KeyError:
            pass

    if codec_audio is not None:
        profil['acodec'] = codec_audio

    return profil


def calculer_resize(width, height, resolution=270):
    # Trouver nouvelles dimensions pour 270p
    if width > height:
        ratio = width / height
        height_resized = resolution
        width_resized = round(resolution * ratio)
    else:
        ratio = height / width
        width_resized = resolution
        height_resized = (resolution * ratio)

    return width_resized, height_resized


async def convertir_progress(etat_media: EtatMedia, job: dict,
                             src_file: tempfile.NamedTemporaryFile,
                             dest_file: tempfile.NamedTemporaryFile,
                             cancel_event: Optional[asyncio.Event] = None) -> Optional[dict]:

    loop = asyncio.get_running_loop()
    dir_staging = etat_media.configuration.dir_staging

    probe_info = await loop.run_in_executor(None, probe_video, src_file.name)

    with tempfile.TemporaryDirectory(dir=dir_staging) as tmpdir:
        socket_filename = os.path.join(tmpdir, 'sock')
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock1:
            sock1.bind(socket_filename)
            sock1.listen(1)

            height = probe_info['height']
            width = probe_info['width']
            resolution = job.get('resolutionVideo') or probe_info['resolution']
            width_resized, height_resized = calculer_resize(width, height, resolution)

            params_output = get_profil(job)
            params_output['vf'] = f'scale={width_resized}:{height_resized}'

            LOGGER.debug("Params output ffmpeg : %s" % params_output)
            # params_output = {
            #     'format': 'webm',
            #     'vcodec': 'libvpx-vp9',
            #     'vf': f'scale={width_resized}:{height_resized}',
            #     'acodec': 'libopus',
            #     'b:a': '128k',
            #     'preset': 'medium',
            #     'crf': 36,  # Quality video
            #     'threads': 3,
            # }
            stream = ffmpeg.input(src_file.name)
            stream = stream.output(dest_file.name, **params_output)
            stream = stream.global_args('-progress', f'unix://{socket_filename}')
            stream = stream.overwrite_output()
            ffmpeg_process = stream.run_async(pipe_stdout=True, pipe_stderr=True)
            try:
                run_ffmpeg = loop.run_in_executor(None, run_stream, ffmpeg_process)
                watcher = loop.run_in_executor(None, _do_watch_progress, probe_info, sock1, progress_handler)
                jobs = [run_ffmpeg, watcher]
                if cancel_event is not None:
                    jobs.append(asyncio.create_task(cancel_event.wait()))
                done, pending = await asyncio.wait(jobs, return_when=asyncio.FIRST_COMPLETED)

                # Verifier si on a au moins une exception
                for t in done:
                    if t.exception():
                        raise t.exception()

                # Verifier si on a un evenement cancel - va forcer l'arret de la job ffmpeg
                if cancel_event is not None and cancel_event.is_set():
                    raise Exception('job ffmpeg annulee')

                ffmpeg_process = None

                return params_output

            except ffmpeg.Error as e:
                # print(e.stderr, file=sys.stderr)
                LOGGER.exception("Erreur ffmpeg : %s" % e)
                raise e
            except Exception as e:
                # print('Exception : %s' % e)
                LOGGER.exception("Erreur transcodage video : %s" % e)
                raise e
            finally:
                if ffmpeg_process is not None:
                    LOGGER.error("Cancelling ffmpeg process %s" % ffmpeg_process.pid)
                    os.kill(ffmpeg_process.pid, signal.SIGINT)
