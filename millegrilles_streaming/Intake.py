import asyncio
import datetime
import json
import logging
import os
import pathlib

from typing import Optional

from millegrilles_streaming import Constantes
from millegrilles_messages.jobs.Intake import IntakeHandler
from millegrilles_messages.MilleGrillesConnecteur import EtatInstance
from millegrilles_streaming.Consignation import ConsignationHandler
from millegrilles_streaming.Configuration import InformationFuuid


LOGGER = logging.getLogger(__name__)

CONST_MAX_RETRIES_CLE = 2


class IntakeJob:

    def __init__(self, info: InformationFuuid, cle_chiffree):
        self.info = info
        self.cle_chiffree = cle_chiffree

    @property
    def fuuid(self):
        return self.info.fuuid


class IntakeStreaming(IntakeHandler):
    """
    Gere le dechiffrage des videos.
    """

    def __init__(self, stop_event: asyncio.Event, etat_instance: EtatInstance,
                 consignation_handler: ConsignationHandler, timeout_cycle: Optional[int] = None):
        super().__init__(stop_event, etat_instance, timeout_cycle)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__consignation_handler = consignation_handler

        self.__jobs: Optional[asyncio.Queue[IntakeJob]] = None

        self.__events_fuuids = dict()

    async def run(self):
        await asyncio.gather(
            super().run(),
            self.entretien_dechiffre_thread(),
            self.entretien_download_thread()
        )

    async def configurer(self):
        self.__jobs = asyncio.Queue(maxsize=5)
        return await super().configurer()

    async def entretien_download_thread(self):
        wait_coro = self._stop_event.wait()
        while self._stop_event.is_set() is False:
            self.__logger.debug("Entretien download")
            path_download = pathlib.Path(self.get_path_download())
            fuuids_supprimes = entretien_download(path_download, self.__events_fuuids)
            await asyncio.wait([wait_coro], timeout=20)

    async def entretien_dechiffre_thread(self):
        wait_coro = self._stop_event.wait()
        while self._stop_event.is_set() is False:
            self.__logger.debug("Entretien dechiffre")
            path_dechiffre = pathlib.Path(self.get_path_dechiffre())
            fuuids_supprimes = entretien_dechiffre(path_dechiffre)
            await asyncio.wait([wait_coro], timeout=300)

    async def traiter_prochaine_job(self) -> Optional[dict]:
        try:
            job = self.__jobs.get_nowait()
            fuuid = job.fuuid
            self.__logger.debug("Traiter job streaming pour fuuid %s" % fuuid)
            await self.traiter_job(job)
        except asyncio.QueueEmpty:
            return None  # Condition d'arret de l'intake
        except Exception as e:
            self.__logger.exception("Erreur traitement job download")
            return {'ok': False, 'err': str(e)}

        return {'ok': True}

    async def annuler_job(self, job: dict, emettre_evenement=False):
        raise NotImplementedError('must override')

    async def traiter_job(self, job):
        fuuid = job.fuuid

        path_download_fichier = pathlib.Path(self.get_path_download(), fuuid + '.work')
        path_download_json = pathlib.Path(self.get_path_download(), fuuid + '.json')

        params_dechiffrage = job.info.get_params_dechiffrage()
        await self.__consignation_handler.download_fichier(fuuid, job.cle_chiffree, params_dechiffrage, path_download_fichier)

        # Download reussi, deplacer les fichiers vers repertoire dechiffre
        path_dechiffre_fichier = pathlib.Path(self.get_path_dechiffre(), fuuid + '.dat')
        path_dechiffre_json = pathlib.Path(self.get_path_dechiffre(), fuuid + '.json')

        os.rename(path_download_fichier, path_dechiffre_fichier)
        os.rename(path_download_json, path_dechiffre_json)

        try:
            event_download = self.__events_fuuids[fuuid]
            event_download.set()
            del self.__events_fuuids[fuuid]
        except KeyError:
            pass  # Ok

    async def __ajouter_job(self, info: InformationFuuid):
        """
        :param info: Fuuid a downloader et dechiffrer.
        :return:
        :raises asyncio.QueueFull: Si q de jobs est pleine.
        """
        fuuid = info.fuuid

        try:
            self.__events_fuuids[fuuid]
        except KeyError:
            pass  # Ok, la job n'existe pas en memoire
        else:
            raise Exception("La job sur fuuid %s existe deja" % fuuid)

        # Creer evenement d'attente pour metter les autres requetes en attente sur ce process
        self.__events_fuuids[fuuid] = asyncio.Event()

        path_download_json = pathlib.Path(os.path.join(self.get_path_download(), fuuid + '.json'))

        # Verifier que le fichier existe sur la consignation (requete HEAD)
        reponse_head = await self.__consignation_handler.verifier_existance(fuuid)
        status_fuuid = reponse_head['status']
        info_fichier = {
            'fuuid': fuuid,
            'mimetype': info.mimetype,
            'status': status_fuuid,
            'taille': reponse_head['taille'],
            'jwt_token': info.jwt_token
        }
        with path_download_json.open(mode='w') as fichier:
            json.dump(info_fichier, fichier)

        if status_fuuid != 200:
            # Le fichier n'est pas disponible. Plus rien a faire
            self.__logger.debug('Fichier %s non disponible sur consignation' % fuuid)
            return info_fichier

        try:
            # Recuperer la cle pour dechiffrer la job
            ref_fuuid = info.ref or info.fuuid
            reponse_cle = None
            for i in range(1, CONST_MAX_RETRIES_CLE+1):
                self.__logger.debug("Recuperer_cle (try %d)" % i)
                try:
                    reponse_cle = await self.recuperer_cle(info.user_id, ref_fuuid, info.jwt_token, timeout=6)
                    break
                except (asyncio.CancelledError, asyncio.TimeoutError) as e:
                    self.__logger.warning("Timeout recuperer_cle (try %d de %d)" % (i, CONST_MAX_RETRIES_CLE))
                    if i == CONST_MAX_RETRIES_CLE:
                        raise e

            cle_chiffree = reponse_cle['cle']

            if info.format is None:
                # On travaille avec le fichier original, copier info chiffrage
                info.format = reponse_cle['format']
                info.header = reponse_cle.get('header')

            self.__logger.debug('Creer la job de download pour fuuid %s' % fuuid)
            job = IntakeJob(info, cle_chiffree)
            self.__jobs.put_nowait(job)
            # S'assurer de demarrer le traitement immediatement
            await self.trigger_traitement()
        except Exception as e:
            # Set event attent et supprimer
            try:
                event_download = self.__events_fuuids[fuuid]
                del self.__events_fuuids[fuuid]
                event_download.set()
            except (AttributeError, KeyError) as e2:
                self.__logger.info("Erreur del info download %s en memoire (%s)" % (fuuid, e2))

            # Cleanup du json, abort le download
            path_download_json.unlink(missing_ok=True)

            raise e

        return info_fichier

    async def recuperer_cle(self, user_id: str, fuuid: str, jwt_token: str, timeout=15) -> dict:
        producer = self._etat_instance.producer
        await asyncio.wait_for(producer.producer_pret().wait(), 3)

        domaine = 'GrosFichiers'
        action = 'getClesStream'
        requete_cle = { 'user_id': user_id, 'fuuids': [fuuid], 'jwt': jwt_token }
        reponse_cle = await producer.executer_requete(
            requete_cle,
            domaine=domaine, action=action, exchange='2.prive', timeout=timeout)
        reponse_parsed = reponse_cle.parsed

        if reponse_parsed['acces'] != '1.permis':
            raise Exception('acces cle refuse : %s' % reponse_parsed['acces'])

        reponse_cle = reponse_parsed['cles'][fuuid]

        # Test pour voir si la cle est dechiffrable
        clecertificat = self._etat_instance.clecertificat
        cle_chiffree = reponse_cle['cle']
        _cle_dechiffree = clecertificat.dechiffrage_asymmetrique(cle_chiffree)

        return reponse_cle

    def get_fichier_dechiffre(self, fuuid) -> Optional[InformationFuuid]:
        """
        :param fuuid: Fuuid du fichier dechiffre.
        :return: L'information pour acceder au fichier dechiffre, incluant metadonnes. None si fichier n'existe pas.
        """
        path_dechiffre_dat = pathlib.Path(os.path.join(self.get_path_dechiffre(), fuuid + '.dat'))

        try:
            stat_dat = path_dechiffre_dat.stat()
        except FileNotFoundError:
            return None

        # Touch le fichier pour indiquer qu'on l'utilise encore
        path_dechiffre_dat.touch()

        # Charger les metadonnees (json associe)
        path_dechiffre_json = pathlib.Path(os.path.join(self.get_path_dechiffre(), fuuid + '.json'))
        with path_dechiffre_json.open() as fichier:
            info_json = json.load(fichier)

        info = InformationFuuid(fuuid, None, info_json)
        info.path_complet = str(path_dechiffre_dat)
        info.taille = stat_dat.st_size

        return info

    def get_progres_download(self, fuuid) -> Optional[InformationFuuid]:
        path_download_json = pathlib.Path(os.path.join(self.get_path_download(), fuuid + '.json'))

        if path_download_json.exists() is False:
            # Le fichier n'est pas en download / traitement
            return None

        with path_download_json.open() as fichier:
            contenu_json = json.load(fichier)

        path_fichier_work = pathlib.Path(os.path.join(self.get_path_download(), fuuid + '.work'))
        try:
            stat_dat = path_fichier_work.stat()
        except FileNotFoundError:
            # On n'a pas de fichier .work. Retourner le contenu du .json (peut avoir un status d'erreur, e.g. 404).
            reponse = InformationFuuid(fuuid, None, contenu_json)
            reponse.position_courante = 0
            return reponse

        # Retourner l'information du fichier avec taille totale et position courante
        reponse = InformationFuuid(fuuid, None, contenu_json)
        reponse.position_courante = stat_dat.st_size

        return reponse

    def get_path_dechiffre(self):
        path_staging = self._etat_instance.configuration.dir_staging
        return os.path.join(path_staging, Constantes.DIR_DECHIFFRE)

    def get_path_download(self):
        path_staging = self._etat_instance.configuration.dir_staging
        return os.path.join(path_staging, Constantes.DIR_DOWNLOAD)

    async def attendre_download(self, fuuid: str, jwt_token: str, params: dict, timeout: Optional[int] = None) -> Optional[InformationFuuid]:
        # Verifier si le fichier est deja dechiffre
        info = self.get_fichier_dechiffre(fuuid)
        if info is not None:
            return info

        # Verifier si le download existe deja
        try:
            event_attente = self.__events_fuuids[fuuid]
        except KeyError:
            info = self.get_progres_download(fuuid)
            if info is None:
                # Creer la job de download
                info = InformationFuuid(fuuid, jwt_token, params)
                await info.init()
                reponse = await self.__ajouter_job(info)
                if reponse['status'] != 200:
                    info.status = reponse['status']
                    return info
            event_attente = self.__events_fuuids[fuuid]

        if timeout is not None:
            done, pending = await asyncio.wait(
                [self._stop_event.wait(), event_attente.wait()], timeout=timeout, return_when=asyncio.tasks.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
            if self._stop_event.is_set():
                raise Exception('done')
        else:
            # Pas de timeout, retourner l'info qu'on a deja
            return info

        info = self.get_fichier_dechiffre(fuuid)
        if info is not None:
            return info

        return self.get_progres_download(fuuid)


def entretien_download(path_download: pathlib.Path, dict_attente: dict, timeout=Constantes.CONST_TIMEOUT_DOWNLOAD) -> list:
    dt_expiration = datetime.datetime.now() - datetime.timedelta(seconds=timeout)
    ts_expiration = dt_expiration.timestamp()

    fuuids_supprimes = list()

    # Supprimer les fichiers .dat (et .json associe)
    for fichier in path_download.iterdir():
        if fichier.match('*.work'):
            fuuid = fichier.name.split('.')[0]
            LOGGER.debug("Verifier expiration fichier download %s" % str(fichier))
            stat_fichier = fichier.stat()
            if stat_fichier.st_mtime < ts_expiration:
                LOGGER.debug("Download %s est expire, on le supprime" % fuuid)

                try:
                    dict_attente[fuuid].set()
                    del dict_attente[fuuid]
                except KeyError:
                    pass  # OK

                fichier.unlink()

                fichier_json = pathlib.Path(str(fichier).replace('.work', '.json'))
                try:
                    fichier_json.unlink()
                except FileNotFoundError:
                    pass  # Ok
                fuuids_supprimes.append(fuuid)

    # Cleanup des .json expires et orphelins
    for fichier in path_download.iterdir():
        if fichier.match('*.json'):
            fuuid = fichier.name.split('.')[0]
            stat_json = fichier.stat()
            if stat_json.st_mtime < ts_expiration:
                # Le json est vieux - verifier s'il existe un fichier .work associe
                fichier_work = pathlib.Path(str(fichier).replace('.json', '.work'))
                if fichier_work.exists() is False:
                    try:
                        dict_attente[fuuid].set()
                        del dict_attente[fuuid]
                    except KeyError:
                        pass  # OK

                    # Le fichier json n'a aucun .dat associe, on supprime
                    LOGGER.warning("Supprimer fichier json orphelin %s" % str(fichier))
                    fichier.unlink()
                    fuuids_supprimes.append(fuuid)

    return fuuids_supprimes


def entretien_dechiffre(path_dechiffre: pathlib.Path, timeout=Constantes.CONST_TIMEOUT_DECHIFFRE):
    pass

    """
    Supprime les fichiers dechiffres qui ont ete supprimes.
    :param path_dechiffre:
    :param timeout:
    :return: Liste de fuuids qui ont ete supprimes
    """

    dt_expiration = datetime.datetime.now() - datetime.timedelta(seconds=timeout)
    ts_expiration = dt_expiration.timestamp()

    fuuids_supprimes = list()

    # Supprimer les fichiers .dat (et .json associe)
    for fichier in path_dechiffre.iterdir():
        if fichier.match('*.dat'):
            LOGGER.debug("Verifier expiration fichier dechiffre %s" % str(fichier))
            stat_fichier = fichier.stat()
            if stat_fichier.st_mtime < ts_expiration:
                # Fichier expire
                LOGGER.debug("Fichier dechiffre est expire %s" % str(fichier))
                fuuid = fichier.name
                fichier.unlink()
                fichier_json = pathlib.Path(str(fichier).replace('.dat', '.json'))
                try:
                    fichier_json.unlink()
                except FileNotFoundError:
                    pass  # Ok
                fuuids_supprimes.append(fuuid)

    # Cleanup des .json orphelins
    for fichier in path_dechiffre.iterdir():
        if fichier.match('*.json'):
            fichier_dat = pathlib.Path(str(fichier).replace('.json', '.dat'))
            if fichier_dat.exists() is False:
                LOGGER.warning("Supprimer fichier json orphelin %s" % str(fichier))
                # Le fichier json n'a aucun .dat associe, on supprime
                fichier.unlink()

    return fuuids_supprimes
