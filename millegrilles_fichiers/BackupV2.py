import logging
import json
import shutil
import struct
import pathlib

from io import BufferedReader
from aiohttp import ClientSession
from ssl import SSLContext

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


async def sync_backups_v2_primaire(path_backups: pathlib.Path, session: ClientSession, ssl_context: SSLContext, url_backup: str):
    info = await _compare_info_domaines_primaire(path_backups, session, ssl_context, url_backup)

    for domaine, info_domaine in info.items():
        if info_domaine.get('download') is True:
            # Download nouveau domaine courant
            await download_backups_v2(path_backups, session, ssl_context, url_backup, domaine, info_domaine)
        elif info_domaine.get('upload') is True:
            # Download domaine comme nouveau courant pour primaire
            await upload_backups_v2(path_backups, session, ssl_context, url_backup, domaine, info_domaine)
        elif info_domaine.get('sync') is True:
            # Sync fichiers pour meme version courante
            await sync_backups_v2(path_backups, session, ssl_context, url_backup, domaine, info_domaine)
        else:
            LOGGER.warning("sync_backups_v2_primaire Aucune action determinee pour domaine %s" % domaine)


async def _compare_info_domaines_primaire(path_backups: pathlib.Path, session: ClientSession, ssl_context: SSLContext, url_backup: str) -> dict:
    # Requete pour domaines, reponse
    domaines_query = '%s/domaines' % url_backup
    response_domaines = await session.get(domaines_query, ssl_context=ssl_context)
    domaines_info = await response_domaines.json()

    # Map domaines par nom
    domaines_dict = dict()
    for domaine_info in domaines_info['domaines']:
        domaines_dict[domaine_info['domaine']] = domaine_info

    # Faire l'inventaire local et combiner a la liste remote
    for local_domaine_path in path_backups.iterdir():
        if local_domaine_path.is_dir():
            nom_domaine = local_domaine_path.name
            info_courant_path = pathlib.Path(local_domaine_path, 'courant.json')
            try:
                with open(info_courant_path, 'rt') as fichier:
                    info_courant = json.load(fichier)
            except FileNotFoundError:
                # Download incomplet, ne compte pas
                continue
            try:
                remote_info = domaines_dict[nom_domaine]
                remote_info['local'] = info_courant
            except KeyError:
                # Domaine manquant sur primaire, on va l'uploader
                domaines_dict[nom_domaine] = {'domaine': nom_domaine, 'upload': True, 'local': info_courant}

    # Determiner les domaines qui ont des versions differentes entre distant et local
    for domaine, domaine_info in domaines_dict.items():
        if domaine_info.get('upload') is True:
            pass    # Le domaine n'existe pas sur le primaire, on va l'uploader
        elif domaine_info.get('local') is None:
            # Le domaine n'existe pas localement, on va le downloader
            domaine_info['download'] = True
        elif domaine_info['concatene']['version'] == domaine_info['local']['version']:
            domaine_info['sync'] = True  # Meme version, sync fichiers
        else:
            # Determiner quelle version "courante" est la plus recente
            date_remote = domaine_info['concatene']['date']
            date_locale = domaine_info['local']['date']
            if date_remote >= date_locale:
                domaine_info['download'] = True
            else:
                domaine_info['upload'] = True

    return domaines_dict


async def download_backups_v2(path_backups: pathlib.Path, session: ClientSession, ssl_context: SSLContext, url_backup: str, domaine: str, info_domaine: dict):
    try:
        version = info_domaine['concatene']['version']
    except KeyError:
        version = info_domaine['local']['version']

    path_version = pathlib.Path(path_backups, domaine, 'archives', version)
    path_version.mkdir(parents=True, exist_ok=True)

    await sync_backups_v2(path_backups, session, ssl_context, url_backup, domaine, info_domaine)

    # Mettre info.json comme nouveau courant.json pour le domaine
    path_courant = pathlib.Path(path_backups, domaine, 'courant.json')
    path_info = pathlib.Path(path_version, 'info.json')
    with open(path_info, 'rb') as fichier:
        with open(path_courant, 'wb') as output:
            output.write(fichier.read())


async def upload_backups_v2(path_backups: pathlib.Path, session: ClientSession, ssl_context: SSLContext, url_backup: str, domaine: str, info_domaine: dict):
    # Synchroniser les fichiers. Le fichier Concatene va declencher la nouvelle version sur le primaire.
    await sync_backups_v2(path_backups, session, ssl_context, url_backup, domaine, info_domaine)


async def sync_backups_v2(path_backups: pathlib.Path, session: ClientSession, ssl_context: SSLContext, url_backup: str, domaine: str, info_domaine: dict):
    try:
        version = info_domaine['concatene']['version']
    except KeyError:
        version = info_domaine['local']['version']

    fichiers_download, fichiers_upload = await backup_v2_listes_fichiers(path_backups, session, ssl_context, url_backup, domaine, version)

    for fichier_download in fichiers_download:
        url_fichier = '%s/%s/archives/%s/%s' % (url_backup, domaine, version, fichier_download)
        path_version = pathlib.Path(path_backups, domaine, 'archives', version)
        path_fichier_backup = pathlib.Path(path_version, fichier_download)
        path_fichier_backup_work = pathlib.Path(path_version, fichier_download + '.work')
        try:
            with open(path_fichier_backup_work, 'wb') as output_file:
                async with session.get(url_fichier, ssl=ssl_context) as resp:
                    resp.raise_for_status()  # Arreter sur toute erreur
                    async for chunk in resp.content.iter_chunked(64 * 1024):
                        output_file.write(chunk)
            path_fichier_backup_work.rename(path_fichier_backup)
            LOGGER.info("sync_backups_v2 Download backup_v2 fichier OK: %s" % fichiers_download)
        finally:
            path_fichier_backup_work.unlink(missing_ok=True)
    else:
        LOGGER.info("sync_backups_v2 Backups sync domaine %s OK, aucuns changements sur version %s" % (domaine, version))

    for fichier_upload in fichiers_upload:
        path_fichier_backup = pathlib.Path(path_backups, domaine, 'archives', version, fichier_upload)
        with open(path_fichier_backup, 'rb') as fichier:
            header = lire_header_archive_backup(fichier)

        if header['type_archive'] == 'I':
            type_fichier = 'incremental'
        elif header['type_archive'] == 'C':
            type_fichier = 'concatene'
        elif header['type_archive'] == 'F':
            type_fichier = 'final'
        else:
            raise Exception("Unsupported archive type: %s" % header['type_archive'])

        url_fichier = '%s/%s/%s/%s/%s' % (url_backup, domaine, type_fichier, version, fichier_upload)
        with open(path_fichier_backup, 'rb') as fichier:
            async with session.put(url_fichier, ssl=ssl_context, data=fichier) as resp:
                resp.raise_for_status()  # Arreter sur toute erreur

    path_local = pathlib.Path(path_backups, domaine, 'archives', version)
    path_local.mkdir(parents=True, exist_ok=True)


async def backup_v2_listes_fichiers(path_backups: pathlib.Path, session: ClientSession, ssl_context: SSLContext, url_backup: str, domaine: str, version: str):
    # Recuperer liste distante
    url_info_domaine = '%s/%s/archives/%s' % (url_backup, domaine, version)
    reponse_remote = await session.get(url_info_domaine, ssl_context=ssl_context)
    fichiers_distant = await reponse_remote.text()

    # Split en set pour retirer fichiers communs avec local
    fichiers_distant = set(fichiers_distant.split('\n'))
    fichiers_distant.add('info.json')
    try:
        fichiers_distant.remove('')  # Cleanup
    except KeyError:
        pass

    fichiers_manquants_remote = list()

    path_archives_domaine = pathlib.Path(path_backups, domaine, 'archives', version)
    try:
        for fichier_local in path_archives_domaine.iterdir():
            nom_fichier = fichier_local.name
            try:
                fichiers_distant.remove(nom_fichier)
            except KeyError:
                fichiers_manquants_remote.append(nom_fichier)
    except FileNotFoundError:
        # Nouveau backup
        fichiers_distant.add('info.json')

    LOGGER.debug("Fichiers manquants: %s\nFichiers a uploader: %s" % (fichiers_distant, fichiers_manquants_remote))

    return list(fichiers_distant), fichiers_manquants_remote
