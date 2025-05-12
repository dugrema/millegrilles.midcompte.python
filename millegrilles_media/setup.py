from setuptools import setup, find_packages
from os import environ

__VERSION__ = environ.get("VBUILD") or "2025.3.0"


setup(
    name='millegrilles_media',
    version=__VERSION__,
    packages=find_packages(),
    url='https://github.com/dugrema/millegrilles.midcompte.python',
    license='AFFERO',
    author='Mathieu Dugre',
    author_email='mathieu.dugre@mdugre.info',
    description='Scripts Python de conversion media pour MilleGrilles',
    install_requires=[
        'pytz>=2020.4',
        'pymongo>=3.11.2,<4.0',
        'aiohttp>=3.8.1,<4',
        'requests>=2.28.1,<3',
        'wand>=0.6,<0.7',
        'ffmpeg-python>=0.2,<0.3'
    ]
)
