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
    ffmpeg \
        .input(VIDEO_FILE, ss=1) \
        .filter('scale', 128, -1) \
        .output('/home/mathieu/tmp/video_thumbnail.jpg', vframes=1) \
        .run()

    stream = ffmpeg.input(VIDEO_FILE, ss=1)


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
