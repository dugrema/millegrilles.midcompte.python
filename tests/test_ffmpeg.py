import asyncio
import datetime
import os
import sys
import tempfile
import socket
import signal

import ffmpeg


VIDEO_1 = '/home/mathieu/Videos/SaveOurLake/005.MOV'
VIDEO_2 = '/home/mathieu/Videos/SaveOurLake/DSC_0751[1].MOV'
VIDEO_3 = '/home/mathieu/tas/tmp/101.mpeg'
OUTPUT_WEBM = '/home/mathieu/tmp/output.webm'
VIDEO_FILE = VIDEO_3
VIDEO_PROBE = VIDEO_3


def probe(file=VIDEO_PROBE):
    probe = ffmpeg.probe(file)
    video_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)
    audio_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'audio'), None)
    width = int(video_stream['width'])
    height = int(video_stream['height'])
    resolution = min(width, height)
    print("Video %s x %s (%dp)" % (width, height, resolution))
    return (width, height, resolution)

def thumbnail():
    with tempfile.NamedTemporaryFile() as tmp_fichier:
        with open(VIDEO_FILE, 'rb') as input:
            tmp_fichier.write(input.read())
        tmp_fichier.seek(0)

        probe = ffmpeg.probe(tmp_fichier.name)
        duration = float(probe['format']['duration'])
        snapshot_position = duration * 0.2
        ffmpeg \
            .input(tmp_fichier.name, ss=snapshot_position) \
            .output('/home/mathieu/tmp/video_thumbnail.jpg', vframes=1) \
            .overwrite_output() \
            .run()


def convertir_h264_270p():
    width, height, resolution = probe(VIDEO_FILE)
    width_resized, height_resized = calculer_resize(width, height)

    params_output = {
        'format': 'mp4',
        'vcodec': 'libx264',
        # 'videoCodecName': 'h264',
        'vf': f'scale={width_resized}:{height_resized}',
        'acodec': 'aac',
        'b:a': '64k',
        'ac': 1,
        'preset': 'veryFast',
        'crf': 30,  # Quality video
        'threads': 3,
    }
    stream = ffmpeg.input(VIDEO_FILE)
    stream = stream.output('/home/mathieu/tmp/output_270p.mp4', **params_output)
    stream.run()


def convertir_vp9():
    width, height, resolution = probe(VIDEO_FILE)
    resolution = min(resolution, 1080)
    width_resized, height_resized = calculer_resize(width, height, resolution)

    params_output = {
        'format': 'webm',
        'vcodec': 'libvpx-vp9',
        'vf': f'scale={width_resized}:{height_resized}',
        'acodec': 'libopus',
        'b:a': '128k',
        'preset': 'medium',
        'crf': 36,  # Quality video
        'threads': 3,
    }
    stream = ffmpeg.input(VIDEO_FILE)
    stream = stream.output('/home/mathieu/tmp/output.webm', **params_output)
    stream.run()


def convertir_hevc():
    width, height, resolution = probe(VIDEO_FILE)
    resolution = min(resolution, 480)
    # width_resized, height_resized = calculer_resize(width, height, resolution)

    if width < height:
        scaling = f'scale=-2:{resolution}'
    else:
        scaling = f'scale={resolution}:-2'

    params_output = {
        'format': 'mp4',
        'vcodec': 'libx265',
        'vf': scaling,
        'acodec': 'eac3',
        'b:a': '128k',
        'preset': 'medium',
        'crf': 30,  # Quality video
        'threads': 3,
    }
    stream = ffmpeg.input(VIDEO_FILE)
    stream = stream.output('/home/mathieu/tmp/output_hevc.mp4', **params_output)
    stream.run()


def convertir_av1():
    width, height, resolution = probe(VIDEO_FILE)
    resolution = min(resolution, 720)
    width_resized, height_resized = calculer_resize(width, height, resolution)

    params_output = {
        'format': 'webm',
        'vcodec': 'libaom-av1',
        # 'vcodec': 'libsvtav1',
        # 'vcodec': 'librav1e',
        'vf': f'scale={width_resized}:{height_resized}',
        'acodec': 'libopus',
        'b:a': '128k',
        'preset': 'fast',
        'crf': 50,  # Quality video
        'threads': 4,
    }
    stream = ffmpeg.input(VIDEO_FILE)
    stream = stream.output('/home/mathieu/tmp/output_av1.webm', **params_output)
    stream.run()


def convertir_pipe_out():
    width, height, resolution = probe(VIDEO_FILE)
    width_resized, height_resized = calculer_resize(width, height)

    params_output = {
        'format': 'mp4',
        'vcodec': 'libx265',
        'vf': f'scale={width_resized}:{height_resized}',
        'acodec': 'libopus',
        'b:a': '64k',
        'ac': 1,
        'preset': 'medium',
        'crf': 30,  # Quality video
        'threads': 4,
    }
    stream = ffmpeg.input(VIDEO_FILE)
    stream = stream.output('pipe:', **params_output)
    out, _ = stream.run(capture_stdout=True)

    with open('/home/mathieu/tmp/output_buffer.mp4') as output_fichier:
        while out.closed is False:
            buffer = out.read(64*1024)
            output_fichier.write(buffer)


async def convertir_progress():

    loop = asyncio.get_running_loop()
    event = asyncio.Event()

    probe_info = ffmpeg.probe(VIDEO_FILE)

    with tempfile.TemporaryDirectory() as tmpdir:
        socket_filename = os.path.join(tmpdir, 'sock')
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock1:
            sock1.bind(socket_filename)
            sock1.listen(1)

            width, height, resolution = probe(VIDEO_FILE)
            resolution = min(resolution, 1080)
            width_resized, height_resized = calculer_resize(width, height, resolution)

            params_output = {
                'format': 'webm',
                'vcodec': 'libvpx-vp9',
                'vf': f'scale={width_resized}:{height_resized}',
                'acodec': 'libopus',
                'b:a': '128k',
                'preset': 'medium',
                'crf': 36,  # Quality video
                'threads': 3,
            }
            stream = ffmpeg.input(VIDEO_FILE)
            stream = stream.output('/home/mathieu/tmp/output_progress.webm', **params_output)
            stream = stream.global_args('-progress', f'unix://{socket_filename}')
            stream = stream.overwrite_output()
            ffmpeg_process = stream.run_async(pipe_stdout=True, pipe_stderr=True)
            try:
                run_ffmpeg = loop.run_in_executor(None, run_stream, ffmpeg_process)
                # watcher = loop.run_in_executor(None, _do_watch_progress, probe_info, sock1, progress_handler)
                watcher = _do_watch_progress(probe_info, sock1, progress_handler)
                wait_event = asyncio.create_task(run_event(event, 300))
                done, pending = await asyncio.wait([run_ffmpeg, watcher, wait_event], return_when=asyncio.FIRST_COMPLETED)

                for t in done:
                    if t.exception():
                        raise t.exception()

                ffmpeg_process = None
            except ffmpeg.Error as e:
                print(e.stderr, file=sys.stderr)
                raise e
            except Exception as e:
                print('Exception : %s' % e)
                raise e
            finally:
                if ffmpeg_process is not None:
                    print("Cancelling ffmpeg")
                    os.kill(ffmpeg_process.pid, signal.SIGINT)


async def run_event(event, timeout=5):
    await asyncio.wait([event.wait()], timeout=timeout)
    event.set()
    print("!!! run event complete !!!")
    raise Exception("done")


def run_stream(process):
    # stream.run(capture_stdout=True, capture_stderr=True)
    out, err = process.communicate(None)
    retcode = process.poll()
    if retcode:
        raise ffmpeg.Error('error', out, err)
    return out, err


async def progress_handler(etat, probe_info, key, value):
    # print("%s = %s" % (key, value))
    if key == 'frame':
        now = datetime.datetime.now().timestamp()
        if etat['dernier_update'] + etat['intervalle_update'] < now:
            progres = round(float(value) / etat['frames'] * 100)
            print(f'Progres : {progres}%')
            etat['dernier_update'] = now


async def _run_connection(connection, probe_info, handler):
    loop = asyncio.get_running_loop()
    video_stream = next((stream for stream in probe_info['streams'] if stream['codec_type'] == 'video'), None)
    etat = {
        'dernier_update': 0,
        'intervalle_update': 3,
        'frames': float(video_stream['nb_frames']),
    }

    data = b''

    try:
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
                await handler(etat, probe_info, key, value)
            data = lines[-1]
    finally:
        connection.close()


async def _do_watch_progress(probe_info, sock, handler):
    """Function to run in a separate gevent greenlet to read progress
    events from a unix-domain socket."""
    # connection, client_address = sock.accept()
    loop = asyncio.get_running_loop()
    connection, _ = await loop.sock_accept(sock)
    await _run_connection(connection, probe_info, handler)


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


async def temp_play():
    with tempfile.TemporaryFile(dir='/mnt/testtmp') as tmp:
        tmp.write(b'tata titi toto\ntoutou\n')

        tmp.seek(0)

        val = b''
        async for chunk in tmp.iter_chunked(1):
            val += chunk

        print('Reading : %s' % val)
        pass


def main():
    # probe()
    thumbnail()
    # convertir_h264_270p()
    # convertir_vp9()
    # convertir_hevc()
    # convertir_av1()
    # convertir_pipe_out()
    # asyncio.run(convertir_progress())
    # asyncio.run(temp_play())


if __name__ == '__main__':
    main()
