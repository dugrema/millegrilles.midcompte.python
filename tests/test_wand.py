from wand.image import Image
from wand.color import Color


IMAGE_1 = "/home/mathieu/Pictures/SaveOurLake galleries/Gallery/001.JPG"
IMAGE_2 = "/home/mathieu/Pictures/SaveOurLake galleries/Gallery/DSC_0096.JPG"
IMAGE_3 = "/home/mathieu/Pictures/IMG_1343.GIF"
VIDEO_1 = "/home/mathieu/Videos/SaveOurLake/005.MOV"
PDF_1 = "/home/mathieu/Pictures/fichier1.pdf"
PDF_2 = "/home/mathieu/Pictures/fichier2.pdf"
WORD_1 = "/home/mathieu/tmp/fichier1.doc"
IMAGE_FILE = IMAGE_3


def load_image():
    with Image(filename=IMAGE_FILE) as img:
        print('Image taille : ', img.size)
        # img.crop(width=40, height=80, gravity='center')


def thumbnail():
    with open(IMAGE_FILE, 'rb') as fichier_in:
        image_bytes = fichier_in.read()

    with Image(blob=image_bytes) as original:
        # Desactiver alpha (PDF, JPG)
        if original.alpha_channel:
            original.background_color = Color('white')
            original.alpha_channel = 'remove'

        with original.convert('jpeg') as img:
            taille = min(*img.size)
            # print("Crop size %d" % taille)
            # img.liquid_rescale(128, 128)
            img.crop(width=taille, height=taille, gravity='center')
            img.resize(128, 128)
            print("Metadata : %s" % img.metadata.items())
            # img.crop(width=128, height=128, gravity='center')
            # img.transform(resize='128x128')

            img.compression_quality = 25
            #img.thumbnail(128, 128)

            img.format = 'jpeg'
            img.strip()  # Retirer metadata
            jpeg_bin = img.make_blob()
            #img.save(filename='/home/mathieu/tmp/output.jpg')

    with open('/home/mathieu/tmp/output1.jpg', 'wb') as fichier:
        fichier.write(jpeg_bin)


def pdf_webp():
    with Image(filename=f'{PDF_2}[0]', resolution=96) as original:
        if original.alpha_channel:
            original.background_color = Color('white')
            original.alpha_channel = 'remove'

        original.strip()
        # original.sequence.clear()

        with original.convert('webp') as img:
            img.save(filename='/home/mathieu/tmp/output_pdf.webp')


def large_webp():
    with Image(filename=f'{IMAGE_FILE}') as original:
        frames = len(original.sequence)
        for i in range(frames-1, 0, -1):
            original.sequence.pop(i)

    #with Image(filename=f'{IMAGE_FILE}[0]') as original:
        if original.alpha_channel:
            original.background_color = Color('white')
            original.alpha_channel = 'remove'

        original.strip()
        # original.sequence.clear()

        with original.convert('webp') as img:
            img.save(filename='/home/mathieu/tmp/output.webp')


def main():
    # load_image()
    # thumbnail()
    # pdf_webp()
    large_webp()


if __name__ == '__main__':
    main()
