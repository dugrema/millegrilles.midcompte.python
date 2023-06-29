import tempfile

import ffmpeg


VIDEO_1 = '/home/mathieu/Videos/SaveOurLake/005.MOV'
VIDEO_2 = '/home/mathieu/Videos/SaveOurLake/DSC_0751[1].MOV'
VIDEO_FILE = VIDEO_2


def probe():
    probe = ffmpeg.probe(VIDEO_FILE)
    video_stream = next((stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)
    width = int(video_stream['width'])
    height = int(video_stream['height'])
    resolution = min(width, height)
    print("Video %s x %s (%dp)" % (width, height, resolution))


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


def convertir1():
    stream = ffmpeg.input(VIDEO_FILE)
    # stream = ffmpeg.hflip(stream)
    stream = ffmpeg.filter('scale', 270, )
    stream = ffmpeg.output(stream, '/home/mathieu/tmp/output.mp4')
    ffmpeg.run(stream)


def main():
    probe()
    thumbnail()
    # convertir1()


if __name__ == '__main__':
    main()
