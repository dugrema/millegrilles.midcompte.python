import asyncio
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


class IntakeJob:

    def __init__(self, info: InformationFuuid):
        self.info = info

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

    async def configurer(self):
        self.__jobs = asyncio.Queue(maxsize=5)
        return await super().configurer()

    async def traiter_prochaine_job(self) -> Optional[dict]:
        self.__logger.debug("Traiter job streaming")
        try:
            job = self.__jobs.get_nowait()
            fuuid = job.fuuid
            await self.__consignation_handler.download_fichier(fuuid)
        except asyncio.QueueEmpty:
            return None  # Condition d'arret de l'intake
        except Exception as e:
            return {'ok': False, 'err': str(e)}

        return {'ok': True}

    async def annuler_job(self, job: dict, emettre_evenement=False):
        raise NotImplementedError('must override')

    async def ajouter_job(self, info: InformationFuuid):
        """
        :param info: Fuuid a downloader et dechiffrer.
        :return:
        :raises asyncio.QueueFull: Si q de jobs est pleine.
        """
        fuuid = info.fuuid

        # S'assurer que le download n'existe pas deja
        download = self.get_progres_download(fuuid)
        if download is not None:
            raise Exception("Download de %s existe deja" % fuuid)
        fichier_pret = self.get_fichier_dechiffre(fuuid)
        if fichier_pret is not None:
            raise Exception("Download de %s est pret" % fuuid)

        path_download_json = pathlib.Path(os.path.join(self.get_path_download(), fuuid + '.json'))

        # Verifier que le fichier existe sur la consignation (requete HEAD)
        reponse_head = await self.__consignation_handler.verifier_existance(fuuid)
        status_fuuid = reponse_head['status']
        info_fichier = {'fuuid': fuuid, 'status': status_fuuid, 'taille': reponse_head['taille']}
        with path_download_json.open(mode='w') as fichier:
            json.dump(info_fichier, fichier)

        if status_fuuid == 200:
            self.__logger.debug('Creer la job de download pour fuuid %s' % fuuid)
            job = IntakeJob(info)
            self.__jobs.put_nowait(job)
            # S'assurer de demarrer le traitement immediatement
            await self.trigger_traitement()
        else:
            # Le fichier n'est pas disponible. Plus rien a faire
            self.__logger.debug('Fichier %s non disponible sur consignation' % fuuid)

        return info_fichier

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

        info = InformationFuuid(fuuid, info_json)
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
            reponse = InformationFuuid(fuuid, contenu_json)
            reponse.position_courante = 0
            return reponse

        # Retourner l'information du fichier avec taille totale et position courante
        reponse = InformationFuuid(fuuid, contenu_json)
        reponse.position_courante = stat_dat.st_size

        return reponse

    def get_path_dechiffre(self):
        path_staging = self._etat_instance.configuration.dir_staging
        return os.path.join(path_staging, Constantes.ENV_DIR_STAGING, Constantes.DIR_DECHIFFRE)

    def get_path_download(self):
        path_staging = self._etat_instance.configuration.dir_staging
        return os.path.join(path_staging, Constantes.ENV_DIR_STAGING, Constantes.DIR_DOWNLOAD)
