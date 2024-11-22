import aiohttp
import asyncio
import datetime
import logging
import os
import pathlib
import signal
import socket
import sys
import tempfile

from asyncio import create_task, TaskGroup
from typing import Optional

import ffmpeg
import multibase

from millegrilles_media.Context import MediaContext
from millegrilles_media.ImagesHandler import traiter_image
from millegrilles_media.TransfertFichiers import uploader_fichier, chiffrer_fichier, filehost_authenticate
from millegrilles_media.VideoUtils import probe_video

LOGGER = logging.getLogger(__name__)


class ProgressHandler:

    def __init__(self, context: MediaContext, job: dict):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__etat_media = context
        self.job = job
        self.__frames = None  # video_info['frames']

        self.dernier_update = 0
        self.intervalle_update = 3
        self.__stop_emission = False

    @property
    def frames(self):
        return self.__frames

    @frames.setter
    def frames(self, frames):
        if isinstance(frames, (float, int)):
            self.__frames = frames
        elif isinstance(frames, str):
            self.__frames = int(frames)

    async def annuler(self):
        self.__stop_emission = True
        await self.__emettre_progres(-1, 'annule')

    async def traiter_event(self, key, value):
        # print("%s = %s" % (key, value))
        if key == 'frame':
            now = datetime.datetime.now().timestamp()
            if self.dernier_update + self.intervalle_update < now:
                if self.frames is not None:
                    progres = round(float(value) / self.frames * 100)
                else:
                    progres = 1  # Progres n'est pas calcule

                self.dernier_update = now
                # self.queue_events.put({key: value, 'progres': progres})
                await self.emettre_progres(progres)

    async def emettre_progres(self, progres_pct, etat='transcodage'):
        if self.__stop_emission:
            return
        await self.__emettre_progres(progres_pct, etat)

    async def __emettre_progres(self, progres_pct, etat='transcodage'):
        self.__logger.debug(f'Progres {progres_pct}%')
        job_id = self.job['job_id']
        user_id = self.job['user_id']
        fuuid = self.job['fuuid']
        tuuid = self.job['tuuid']
        params = self.job['params']
        # mimetype_conversion = params['cle_conversion'].split(';')[0]
        mimetype_conversion = params['mimetype']
        progres = progres_pct
        if progres > 100.0:
            progres = 100  # Plafond a 100
        evenement = {
            'job_id': job_id,
            'tuuid': tuuid,
            'fuuid': fuuid,
            'user_id': user_id,
            'mimetype': mimetype_conversion,
            'videoCodec': params['codecVideo'],
            'videoQuality': params['qualityVideo'],
            'resolution': params['resolutionVideo'],
            'height': params['resolutionVideo'],
            'pctProgres': progres,
            'etat': etat,
        }

        producer = await self.__etat_media.get_producer()
        await producer.event(evenement, domain='media', action='transcodageProgres', partition=user_id, exchange='2.prive')


class VideoConversionJob:

    def __init__(self, context: MediaContext, job: dict, tmp_file: tempfile.NamedTemporaryFile):
        self.context = context
        self.job = job
        self.tmp_file = tmp_file

        self.progress_handler: Optional[ProgressHandler] = None
        self.cancel_event: Optional[asyncio.Event] = None
        self.termine = False

    async def traiter_video(self):
        self.cancel_event = asyncio.Event()
        self.__inject_params()
        self.progress_handler = ProgressHandler(self.context, self.job)
        await self.__process()

    def __inject_params(self):
        params = self.job['params']
        if params.get('defaults'):
            # Apply low resolution fallback video format for browsers
            params['mimetype'] = 'video/mp4'
            params['codecVideo'] = 'h264'
            params['qualityVideo'] = 28
            params['resolutionVideo'] = 270

    async def annuler(self):
        if not self.termine:
            self.cancel_event.set()
            await self.progress_handler.annuler()

    async def __process(self):
        """
        Transcode un video
        :return:
        """
        params = self.job['params']
        try:
            flag_thumbnails = params['thumbnails']
        except KeyError:
            info_probe = None
            pass  # No thumbnails to create
        else:
            info_probe = await traiter_poster_video(self.job, self.tmp_file, self.context)
            self.tmp_file.seek(0)

        await self.__transcode(info_probe)

    async def __transcode(self, info_probe: Optional[dict]):
        dir_staging = self.context.configuration.dir_staging
        tmp_transcode = tempfile.NamedTemporaryFile(dir=dir_staging)
        try:
            # Convertir le video
            params_conversion = await transcoder_video(
                self.context, self.job, info_probe, self.tmp_file, tmp_transcode, self.progress_handler, self.cancel_event)
            mimetype_output = 'video/%s' % params_conversion['format']
            self.job['mimetype_output'] = mimetype_output

            # Fermer le fichier original (supprime le tmp file)
            self.tmp_file.close()

            with tempfile.TemporaryFile(dir=dir_staging) as tmp_output_chiffre:
                # Chiffrer output
                info_chiffrage = await chiffrer_video(self.context, self.job, tmp_transcode, tmp_output_chiffre)

                # Fermer fichier dechiffre
                tmp_transcode.close()
                tmp_transcode = None

                # Uploader fichier chiffre
                tmp_output_chiffre.seek(0)
                await uploader_video(self.context, self.job, info_chiffrage, tmp_output_chiffre)

            self.termine = True
        finally:
            if tmp_transcode is not None:
                tmp_transcode.close()


async def chiffrer_video(context: MediaContext, job: dict, tmp_input: tempfile.NamedTemporaryFile, tmp_output: tempfile.TemporaryFile) -> Optional[dict]:
    loop = asyncio.get_running_loop()

    LOGGER.debug("probe fichier video transcode : %s" % tmp_input.name)
    tmp_input.seek(0)
    tmp_input.flush()
    info_video = await probe_video(pathlib.Path(tmp_input.name))

    # clecert = context.signing_key
    # cle = job['cle']
    # cle_bytes = clecert.dechiffrage_asymmetrique(cle['cle'])
    # info_dechiffrage = job['cle']
    # cle_bytes: bytes = multibase.decode('m' + info_dechiffrage['cle_secrete_base64'])
    cle_bytes: bytes = job['decrypted_key']
    # cle_id = info_dechiffrage['cle_id']
    cle_id = job['cle_id']

    tmp_input.seek(0)
    info_chiffrage = await chiffrer_fichier(cle_bytes, tmp_input, tmp_output)

    mimetype = job['mimetype_output']

    info_fichier = {
        'hachage': info_chiffrage['hachage'],
        'mimetype': mimetype,
        'taille_fichier': info_chiffrage['taille_chiffree'],
        'taille_originale': info_chiffrage['taille_dechiffree'],
        'nonce': multibase.encode('base64', info_chiffrage['header']).decode('utf-8')[1:],  # Retirer 'm' multibase
        'cle_id': cle_id,
        'format': 'mgs4',
    }

    info_video.update(info_fichier)

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


async def _do_watch_progress(sock, handler):
    """Function to run in a separate gevent greenlet to read progress
    events from a unix-domain socket."""
    loop = asyncio.get_running_loop()

    connection, client_address = await loop.sock_accept(sock)
    data = b''

    # video_stream = next((stream for stream in probe_info['streams'] if stream['codec_type'] == 'video'), None)
    # etat = {
    #     'dernier_update': 0,
    #     'intervalle_update': 3,
    #     'frames': float(video_info['frames']),
    # }
    try:
        await handler(0, 'transcodageDebut')

        while True:
            more_data = await loop.sock_recv(connection, 16)
            if not more_data:
                break
            data += more_data
            lines = data.split(b'\n')
            for line in lines[:-1]:
                line = line.decode()
                parts = line.split('=')
                key = parts[0] if len(parts) > 0 else None
                value = parts[1] if len(parts) > 1 else None
                await handler(key, value)
            data = lines[-1]
    finally:
        connection.close()


async def uploader_video(context: MediaContext, job, info_chiffrage, tmp_output_chiffre):
    commande_associer = preparer_commande_associer(job, info_chiffrage)

    # Uploader les fichiers temporaires
    tmp_output_chiffre.seek(0)
    timeout = aiohttp.ClientTimeout(connect=5, total=600)
    taille_fichier: int = info_chiffrage['taille_fichier']
    # connector = aiohttp.TCPConnector(ssl=context.ssl_context)
    connector = context.get_tcp_connector()
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        session.verify = context.tls_method != 'nocheck'

        await filehost_authenticate(context, session)
        await uploader_fichier(session, context, commande_associer['fuuid_video'], taille_fichier, tmp_output_chiffre)

    # Transmettre commande associer
    producer = await context.get_producer()
    await producer.command(commande_associer, domain='GrosFichiers', action='associerVideo', exchange='3.protege')


def preparer_commande_associer(job: dict, info_chiffrage: dict) -> dict:
    params = job['params']
    profil = job['profil']

    codec_video = profil['vcodec']
    codec_audio = profil['acodec']

    cle_conversion = f'{params['mimetype']};{codec_video};{params['resolutionVideo']}p;{params['qualityVideo']}'

    commande = {
        "job_id": job['job_id'],  # Ajoute dans 2024.9
        "tuuid": job['tuuid'],
        "fuuid": job['fuuid'],
        "user_id": job['user_id'],
        "quality": params['qualityVideo'],

        "codec": codec_video,
        "codec_audio": codec_audio,
        "fuuid_video": info_chiffrage['hachage'],
        "mimetype": info_chiffrage['mimetype'],
        "height": info_chiffrage['height'],
        "width": info_chiffrage['width'],

        "taille_fichier": info_chiffrage['taille_fichier'],
        "hachage": info_chiffrage['hachage'],
        # "header": info_chiffrage['header'],
        "nonce": info_chiffrage['nonce'],
        "cle_id": info_chiffrage['cle_id'],
        "format": info_chiffrage['format'],

        # "cle_conversion": params['cle_conversion'],  # Ajouter dans 2023.7.4
        "cle_conversion": cle_conversion,
    }

    if params.get('thumbnails') is True:
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

    params = job['params']
    codec_video = params['codecVideo']
    try:
        codec_audio = params['codecAudio']
        if codec_audio == 'opus':
            # Patch - utiliser libopus
            codec_audio = 'libopus'
    except KeyError:
        codec_audio = None

    profil = PROFIL_DEFAUT[codec_video].copy()

    for key, mapping in MAPPING_PARAMS.items():
        try:
            profil[mapping] = params[key]
        except KeyError:
            pass

    if codec_audio is not None:
        profil['acodec'] = codec_audio

    return profil


def calculer_resize(width, height, resolution=270):
    # Trouver nouvelles dimensions pour la resolution
    if width > height:
        ratio = width / height
        height_resized = resolution
        width_resized = round(resolution * ratio)
    else:
        ratio = height / width
        width_resized = resolution
        height_resized = round(resolution * ratio)

    # Certains formats (dont VP9) requierent des dimensions paires
    if width_resized % 2 == 1:
        width_resized = width_resized + 1

    if height_resized % 2 == 1:
        height_resized = height_resized + 1

    return width_resized, height_resized


ARGS_OVERRIDE_GEOLOC = [
  '-movflags', 'faststart',
  '-metadata', 'COM.APPLE.QUICKTIME.LOCATION.ISO6709=',
  '-metadata', 'location=',
  '-metadata', 'location-eng='
]

async def emettre_progres_thread(stop: asyncio.Event, progress_handler: ProgressHandler):
    while stop.is_set() is False:
        await progress_handler.emettre_progres(0, 'probe')
        try:
            await asyncio.wait_for(stop.wait(), 10)
            return  # Done
        except asyncio.TimeoutError:
            pass

async def transcoder_video(context: MediaContext, job: dict,
                           probe_info: Optional[dict],
                           src_file: tempfile.NamedTemporaryFile,
                           dest_file: tempfile.NamedTemporaryFile,
                           progress_handler: ProgressHandler,
                           cancel_event: Optional[asyncio.Event] = None) -> Optional[dict]:

    dir_staging = context.configuration.dir_staging

    LOGGER.debug("convertir_progress executer probe")
    if probe_info is None:
        async with TaskGroup() as group:
            stop = asyncio.Event()
            group.create_task(emettre_progres_thread(stop, progress_handler))
            task_probe = group.create_task(probe_video(src_file.name))
            try:
                probe_info = await task_probe
            finally:
                stop.set()

    LOGGER.debug("convertir_progress probe info %s" % probe_info)
    try:
        progress_handler.frames = probe_info['frames']
    except KeyError:
        LOGGER.info("Nombre de frames non disponible pour video, progres ne sera pas montre")

    with tempfile.TemporaryDirectory(dir=dir_staging) as tmpdir:
        socket_filename = os.path.join(tmpdir, 'sock')
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock1:
            sock1.bind(socket_filename)
            sock1.listen(1)

            height = probe_info['height']
            width = probe_info['width']
            params = job['params']
            resolution = params.get('resolutionVideo') or probe_info['resolution']

            if width > height:
                scaling = f'scale=-2:{resolution}'
            else:
                scaling = f'scale={resolution}:-2'

            if params.get('defaults') == 'true':
                # Verifier si on a des sous-titres de detectes
                try:
                    add_subtitles = probe_info['subtitles'] is not None
                except KeyError:
                    # Aucuns sous-titres
                    add_subtitles = False
            elif params.get('subtitles'):
                add_subtitles = True
            else:
                add_subtitles = False

            params_output = get_profil(job)

            job['profil'] = params_output
            if params_output['vcodec'] == 'hevc':
                # Metatadata pour ios
                params_output['tag:v'] = 'hvc1'

            if add_subtitles:
                # vf += f",subtitles='{src_file.name}':si=0"
                params_output['filter_complex'] = f'[0:v][0:s]overlay[ov];[ov]{scaling}[v]'
                params_output['map'] = ['[v]', '0:a']
                pass
            else:
                params_output['vf'] = [scaling]

            args_output = ARGS_OVERRIDE_GEOLOC.copy()
            args_output.append('-progress')
            args_output.append(f'unix://{socket_filename}')

            LOGGER.debug("ffmpeg params output : %s" % params_output)
            stream = ffmpeg.input(src_file.name)
            stream = stream.output(dest_file.name, **params_output)
            stream = stream.global_args(*args_output)
            stream = stream.overwrite_output()

            LOGGER.debug("FFMPEG args:\n%s" % stream.get_args())

            ffmpeg_process = stream.run_async(pipe_stdout=True, pipe_stderr=True)
            try:
                run_ffmpeg = asyncio.create_task(asyncio.to_thread(run_stream, ffmpeg_process))
                watcher = asyncio.create_task(_do_watch_progress(sock1, progress_handler.traiter_event))
                jobs = [run_ffmpeg, watcher]
                if cancel_event is not None:
                    jobs.append(asyncio.create_task(cancel_event.wait()))

                LOGGER.debug("Running ffmpeg")
                done, pending = await asyncio.wait(jobs, return_when=asyncio.FIRST_COMPLETED)
                LOGGER.debug("Running ffmpeg done")

                # Verifier si on a au moins une exception
                for t in done:
                    if t.exception():
                        await progress_handler.emettre_progres(-1, 'erreur')
                        raise t.exception()

                # Verifier si on a un evenement cancel - va forcer l'arret de la job ffmpeg
                if cancel_event is not None and cancel_event.is_set():
                    await progress_handler.emettre_progres(-1, 'cancelled')
                    raise Exception('job ffmpeg annulee')

                ffmpeg_process = None

                await progress_handler.emettre_progres(100, 'termine')

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


async def traiter_poster_video(job, tmp_file_video: tempfile.TemporaryFile, context: MediaContext) -> dict:
    """
    Genere un thumbnail/small jpg et poster webp
    :param job:
    :param tmp_file_video:
    :param context:
    :return:
    """
    # Extraire un snapshot de reference du video
    dir_staging = context.configuration.dir_staging
    tmp_file_snapshot = tempfile.NamedTemporaryFile(dir=dir_staging, suffix='.jpg')
    try:
        info_probe = await probe_video(pathlib.Path(tmp_file_video.name))
        try:
            duration = info_probe['duration']
            snapshot_position = int(duration * 0.2) + 1
            if snapshot_position > duration:
                snapshot_position = 0
        except KeyError:
            snapshot_position = 5  # Mettre a 5 secondes, duree non disponible

        stream = ffmpeg \
            .input(tmp_file_video.name, ss=snapshot_position) \
            .output(tmp_file_snapshot.name, vframes=1) \
            .overwrite_output()

        await asyncio.to_thread(stream.run)

        # Traiter et uploader le snapshot
        await traiter_image(job, tmp_file_snapshot, context, info_video=info_probe)

        return info_probe
    finally:
        if tmp_file_snapshot.closed is False:
            tmp_file_snapshot.close()
