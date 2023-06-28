from wand.image import Image


IMAGE_1 = "/home/mathieu/Pictures/SaveOurLake galleries/Gallery/001.JPG"
IMAGE_2 = "/home/mathieu/Pictures/SaveOurLake galleries/Gallery/DSC_0096.JPG"
VIDEO_1 = "/home/mathieu/Videos/SaveOurLake/005.MOV"
PDF_1 = "/home/mathieu/Pictures/fichier1.pdf"
PDF_2 = "/home/mathieu/Pictures/fichier2.pdf"
WORD_1 = "/home/mathieu/tmp/fichier1.doc"
IMAGE_FILE = IMAGE_2


def load_image():
    with Image(filename=IMAGE_FILE) as img:
        print('Image taille : ', img.size)
        # img.crop(width=40, height=80, gravity='center')


def thumbnail():
    image_file = '%s[0]' % IMAGE_FILE

    with open(IMAGE_FILE, 'rb') as fichier_in:
        image_bytes = fichier_in.read()

    with Image(blob=image_bytes) as original:
        image_bytes = None

    #with Image(filename=image_file) as original:

        # Desactiver alpha (PDF, JPG)
        if original.alpha_channel:
            original.alpha_channel = False

        with original.convert('jpeg') as img:
            # taille = min([s for s in img.size])
            # print("Crop size %d" % taille)
            # img.liquid_rescale(128, 128)
            # img.crop(width=taille, height=taille, gravity='center')
            # img.resize(128, 128)
            # img.crop(width=128, height=128, gravity='center')
            # img.transform(resize='128x128')

            img.compression_quality = 25
            img.thumbnail(128, 128)

            img.format = 'jpeg'
            jpeg_bin = img.make_blob()
            #img.save(filename='/home/mathieu/tmp/output.jpg')

    with open('/home/mathieu/tmp/output1.jpg', 'wb') as fichier:
        fichier.write(jpeg_bin)



def main():
    # load_image()
    thumbnail()


if __name__ == '__main__':
    main()
