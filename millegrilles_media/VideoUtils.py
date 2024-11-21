import asyncio
import ffmpeg
import logging

LOGGER = logging.getLogger(__name__)


async def probe_video(filepath, count_frames=False) -> dict:
    info_probe = ffmpeg.probe(filepath)

    # Verifier si on a le nombre de frames
    video_stream = next(
        (stream for stream in info_probe['streams'] if stream['codec_type'] == 'video'),
        None
    )

    if count_frames and video_stream.get('nb_frames') is None:
        # Compter le nombre de frames (plus lent)
        try:
            probe_info_read = asyncio.to_thread(ffmpeg.probe, filepath,
                                                select_streams='v:0', count_frames=None, show_entries='stream=nb_read_frames')
            # Injecter le nombre de frames dans le probe_info precedent
            nb_read_frames = probe_info_read['streams'][0]['nb_read_frames']
            video_stream['nb_frames'] = nb_read_frames
        except ffmpeg.Error:
            LOGGER.exception("probe_video Erreur FFMPEG")
        except:
            LOGGER.exception("probe_video Erreur fallback sur count_frames, nombre de frames inconnu")

    info_video = dict()

    video_stream = next((stream for stream in info_probe['streams'] if stream['codec_type'] == 'video'), None)
    try:
        audio_stream = next([s for s in info_probe['streams'] if s['codec_type'] == 'audio'].__iter__())
    except StopIteration:
        audio_stream = None

    try:
        info_video['duration'] = float(info_probe['format']['duration'])
    except KeyError:
        LOGGER.info("Duree du video non disponible")

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

    # Detecter streams audio et subtitles
    audio = list()
    subtitles = list()
    try:
        streams = info_probe['streams']
        for idx in range(0, len(streams)):
            stream = streams[idx]
            try:
                codec_type = stream['codec_type']
            except KeyError:
                continue  # Aucun type identifie

            if codec_type == 'subtitle':
                subtitle_info = {'index': idx}
                try:
                    subtitle_info['language'] = stream['tags']['language']
                except KeyError:
                    pass
                subtitles.append(subtitle_info)
            elif codec_type == 'audio':
                audio_info = {'index': idx}
                try:
                    audio_info['codec_name'] = stream['codec_name']
                except KeyError:
                    pass
                try:
                    bit_rate = stream['bit_rate']
                    if isinstance(bit_rate, str):
                        bit_rate = int(bit_rate)
                    audio_info['bit_rate'] = bit_rate
                except (KeyError, ValueError):
                    pass
                try:
                    audio_info['default'] = stream['disposition']['default'] == 1
                except KeyError:
                    pass
                try:
                    audio_info['language'] = stream['tags']['language']
                except KeyError:
                    pass
                try:
                    title: str = stream['tags']['title']
                    title = title.replace("\"", "")
                    title = title.strip()
                    audio_info['title'] = title
                except KeyError:
                    pass
                audio.append(audio_info)

        if len(subtitles) > 0:
            info_video['subtitles'] = subtitles

        if len(audio) > 0:
            info_video['audio'] = audio

    except KeyError:
        pass  # Aucuns streams, doit etre invalide (pas de video, audio)

    return info_video
