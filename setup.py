from setuptools import setup, find_packages
import subprocess


def get_version():
    try:
        with open('version.txt', 'r') as version_file:
            version = version_file.readline().strip()
    except FileNotFoundError as e:
        with open('build.txt', "r") as buildno_file:
            build_no = buildno_file.read().strip()

#        commande_git_version = ['git', 'name-rev', '--name-only', 'HEAD']
        commande_git_version = ['git', 'rev-parse', '--abbrev-ref', 'HEAD']
        output_process = subprocess.run(commande_git_version, stdout=subprocess.PIPE)
        version = output_process.stdout.decode('utf8').strip()
        version = '%s.%s' % (version, build_no)
        print("Version: %s" % (version))

    return version


setup(
    name='millegrilles_midcompte',
    version='%s' % get_version(),
    packages=find_packages(),
    url='https://github.com/dugrema/millegrilles.midcompte.python',
    license='AFFERO',
    author='Mathieu Dugre',
    author_email='mathieu.dugre@mdugre.info',
    description='Scripts Python de gestion middleware (RabbitMQ, MongoDB) pour MilleGrilles',
    install_requires=[
        'pytz>=2020.4',
        'pymongo>=3.11.2,<4.0',
        'aiohttp>=3.8.1,<4',
        'requests>=2.28.1,<3'
    ]
)
