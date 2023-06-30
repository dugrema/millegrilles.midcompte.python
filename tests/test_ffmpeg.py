import asyncio
import os
import sys
import tempfile
import socket
import signal

import ffmpeg


VIDEO_1 = '/home/mathieu/Videos/SaveOurLake/005.MOV'
VIDEO_2 = '/home/mathieu/Videos/SaveOurLake/DSC_0751[1].MOV'
VIDEO_FILE = VIDEO_2


def probe(file=VIDEO_FILE):
    probe = ffmpeg.probe(file)
    video_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)
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
    resolution = min(resolution, 1080)
    width_resized, height_resized = calculer_resize(width, height, resolution)

    params_output = {
        'format': 'mp4',
        'vcodec': 'libx265',
        'vf': f'scale={width_resized}:{height_resized}',
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
            stream = stream.output('/home/mathieu/tmp/output.webm', **params_output)
            stream = stream.global_args('-progress', f'unix://{socket_filename}')
            stream = stream.overwrite_output()
            ffmpeg_process = stream.run_async(pipe_stdout=True, pipe_stderr=True)
            try:
                run_ffmpeg = loop.run_in_executor(None, run_stream, ffmpeg_process)
                # run_ffmpeg = stream.run_async(quiet=True)
                watcher = loop.run_in_executor(None, _do_watch_progress, sock1, progress_handler)
                wait_event = asyncio.create_task(run_event(event))
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

def progress_handler(key, value):
    if key == 'frame':
        print("%s = %s" % (key, value))


def _do_watch_progress(sock, handler):
    """Function to run in a separate gevent greenlet to read progress
    events from a unix-domain socket."""
    connection, client_address = sock.accept()
    data = b''
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
                handler(key, value)
            data = lines[-1]
    finally:
        connection.close()


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


def main():
    # probe()
    # thumbnail()
    # convertir_h264_270p()
    # convertir_vp9()
    # convertir_hevc()
    # convertir_av1()
    # convertir_pipe_out()
    asyncio.run(convertir_progress())


if __name__ == '__main__':
    main()