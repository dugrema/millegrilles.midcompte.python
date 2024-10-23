import logging
import json
import shutil
import struct
import pathlib

from io import BufferedReader


LOGGER = logging.getLogger(__name__)


def lire_header_archive_backup(fp: BufferedReader) -> dict:
    fp.seek(0)

    version_header_info = fp.read(4)
    version, header_len = struct.unpack("HH", version_header_info)
    LOGGER.debug("Version %d, Header length %d", version, header_len)

    header_bytes = fp.read(header_len)
    pos_end = header_bytes.find(0x0)
    header_str = header_bytes[0:pos_end].decode('utf-8')

    LOGGER.info("Header\n*%s*" % header_str)
    return json.loads(header_str)


def extraire_headers(archive_path: pathlib.Path) -> list[dict]:
    headers = list()
    for fichier in archive_path:
        if fichier.is_file() is False:
            continue  # Skip
        with open(fichier, 'rt') as fp:
            header = lire_header_archive_backup(fp)
        headers.append(header)
    return headers


async def rotation_backups_v2(backup_path: pathlib.Path, nombre_archives=3):
    for path_domaine in backup_path.iterdir():
        if path_domaine.is_dir():
            await rotation_backups_v2_domaine(path_domaine, nombre_archives)


async def rotation_backups_v2_domaine(domaine_path: pathlib.Path, nombre_archives: int):
    path_courant = pathlib.Path(domaine_path, 'courant.json')
    path_archives = pathlib.Path(domaine_path, 'archives')
    try:
        with open(path_courant) as fichier:
            info_courant = json.load(fichier)
        version_courante = info_courant['version']
    except FileNotFoundError:
        version_courante = None

    versions = list()

    # Extraire information du header de chaque fichier pour trouver
    for version_path in path_archives.iterdir():
        version = version_path.name
        if version_path.name == version_courante:
            continue  # On ne touche pas a la version courante

        # Trouver la plus recente transaction dans cette version, cumuler le nombre total
        nombre_transactions = 0
        fin_backup = 0

        headers = extraire_headers(path_archives)
        for h in headers:
            nombre_transactions = nombre_transactions + h['nombre_transactions']
            if fin_backup < h['fin_backup']:
                fin_backup = h['fin_backup']

        # Conserver information version
        versions.append({'version': version, 'nombre_transactions': nombre_transactions, 'fin_backup': fin_backup, 'version_path': version_path})

    # Trier en ordre invers de fin version
    versions_triees = sorted(versions, key=lambda x: x['fin_backup'], reverse=True)

    # Sanity check, on ne doit pas regresser en nombre de transactions
    nombre_transactions = None
    for v in versions_triees:
        if nombre_transactions is not None:
            if nombre_transactions < v['nombre_transactions']:
                raise Exception('Erreur dans les versions de backup domaine %s, diminution du nombre de transactions' % domaine_path.name)
        nombre_transactions = v['nombre_transactions']

    # Decider de quelles versions on peut supprimer
    versions_supprimer = versions[nombre_archives:]

    for supprimer_version in versions_supprimer:
        version_path = supprimer_version['version_path']
        LOGGER.info("Supprimer backup_v2 domaine %s version %s" % (domaine_path.name, supprimer_version['version']))
        # shutil.rmtree(version_path)
