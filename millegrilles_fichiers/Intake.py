import asyncio
import datetime
import errno
import json
import logging
import os
import pathlib
import shutil

from typing import Optional

from millegrilles_messages.messages import Constantes as ConstantesMillegrille
from millegrilles_messages.messages.Hachage import VerificateurHachage, ErreurHachage
from millegrilles_messages.jobs.Intake import IntakeHandler
from millegrilles_messages.MilleGrillesConnecteur import EtatInstance

from millegrilles_fichiers import Constantes
from millegrilles_fichiers.Consignation import ConsignationHandler, InformationFuuid

LOGGER = logging.getLogger(__name__)

# CONST_MAX_RETRIES_CLE = 2


class IntakeJob:

    def __init__(self, fuuid: str, path_job: pathlib.Path):
        self.fuuid = fuuid
        self.path_job = path_job


class IntakeFichiers(IntakeHandler):
    """
    Gere le dechiffrage des videos.
    """

    def __init__(self, stop_event: asyncio.Event, etat_instance: EtatInstance,
                 consignation_handler: ConsignationHandler, timeout_cycle: Optional[int] = None):
        super().__init__(stop_event, etat_instance, timeout_cycle)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__consignation_handler = consignation_handler

        self.__events_fuuids = dict()

        self.__path_intake = pathlib.Path(self._etat_instance.configuration.dir_consignation, Constantes.DIR_STAGING_INTAKE)
        self.__path_intake.mkdir(parents=True, exist_ok=True)

    def get_path_intake_fuuid(self, fuuid: str):
        return pathlib.Path(self.__path_intake, fuuid)

    async def run(self):
        await asyncio.gather(
            super().run(),
            self.trigger_regulier(),
            # self.entretien_dechiffre_thread(),
            # self.entretien_download_thread()
        )

    async def trigger_regulier(self):
        wait_coro = asyncio.create_task(self._stop_event.wait())

        # Declenchement initial du traitement (recovery)
        done, pending = await asyncio.wait([wait_coro], timeout=5)

        while self._stop_event.is_set() is False:
            await self.trigger_traitement()
            done, pending = await asyncio.wait(pending, timeout=300)

        for p in pending:
            p.cancel()
            try:
                await p
            except asyncio.CancelledError:
                pass  # OK

    async def configurer(self):
        return await super().configurer()

    # async def entretien_download_thread(self):
    #     wait_coro = self._stop_event.wait()
    #     while self._stop_event.is_set() is False:
    #         self.__logger.debug("Entretien download")
    #         path_download = pathlib.Path(self.get_path_download())
    #         fuuids_supprimes = entretien_download(path_download, self.__events_fuuids)
    #         await asyncio.wait([wait_coro], timeout=20)

    # async def entretien_dechiffre_thread(self):
    #     wait_coro = self._stop_event.wait()
    #     while self._stop_event.is_set() is False:
    #         self.__logger.debug("Entretien dechiffre")
    #         path_dechiffre = pathlib.Path(self.get_path_dechiffre())
    #         fuuids_supprimes = entretien_dechiffre(path_dechiffre)
    #         await asyncio.wait([wait_coro], timeout=300)

    async def traiter_prochaine_job(self) -> Optional[dict]:
        try:
            repertoires = repertoires_par_date(self.__path_intake)
            path_repertoire = repertoires[0].path_fichier
            fuuid = path_repertoire.name
            repertoires = None
            self.__logger.debug("traiter_prochaine_job Traiter job intake fichier pour fuuid %s" % fuuid)
            path_repertoire.touch()  # Touch pour mettre a la fin en cas de probleme de traitement
            job = IntakeJob(fuuid, path_repertoire)
            await self.traiter_job(job)
        except IndexError:
            return None  # Condition d'arret de l'intake
        except FileNotFoundError as e:
            raise e  # Erreur fatale
        except Exception as e:
            self.__logger.exception("traiter_prochaine_job Erreur traitement job download")
            return {'ok': False, 'err': str(e)}

        return {'ok': True}

    async def annuler_job(self, job: dict, emettre_evenement=False):
        raise NotImplementedError('must override')

    async def traiter_job(self, job):
        await self.handle_retries(job)

        # Reassembler et valider le fichier
        args = [job]
        path_fichier = await asyncio.to_thread(reassembler_fichier, *args)

        # Consigner : deplacer fichier vers repertoire final
        await self.__consignation_handler.consigner(path_fichier, job.fuuid)

        # Charger transactions, cles. Emettre.
        await self.emettre_transactions(job)

        if self._etat_instance.est_primaire is False:
            self.__logger.debug("traiter_job Fichier consigne sur secondaire - s'assurer que le fichier existe sur primaire")
            await self.__consignation_handler.ajouter_upload_secondaire(job.fuuid)

        # Supprimer le repertoire de la job
        shutil.rmtree(job.path_job)

    async def handle_retries(self, job: IntakeJob):
        path_repertoire = job.path_job
        fuuid = job.fuuid
        path_fichier_retry = pathlib.Path(path_repertoire, 'retry.json')

        # Conserver marqueur pour les retries en cas d'erreur
        try:
            with open(path_fichier_retry, 'rt') as fichier:
                info_retry = json.load(fichier)
        except FileNotFoundError:
            info_retry = {'retry': -1}

        if info_retry['retry'] > 3:
            self.__logger.error("Job %s irrecuperable, trop de retries" % fuuid)
            shutil.rmtree(path_repertoire)
            raise Exception('too many retries')
        else:
            info_retry['retry'] += 1
            with open(path_fichier_retry, 'wt') as fichier:
                json.dump(info_retry, fichier)

    async def ajouter_upload(self, path_upload: pathlib.Path):
        """ Ajoute un upload au intake. Transfere path source vers repertoire intake. """
        path_etat = pathlib.Path(path_upload, Constantes.FICHIER_ETAT)
        path_fichier_str = str(path_etat)
        self.__logger.debug("Charger fichier %s" % path_fichier_str)
        with open(path_fichier_str, 'rt') as fichier:
            etat = json.load(fichier)
        fuuid = etat['hachage']
        path_intake = self.get_path_intake_fuuid(fuuid)

        # S'assurer que le repertoire parent existe
        path_intake.parent.mkdir(parents=True, exist_ok=True)

        # Deplacer le repertoire d'upload vers repertoire intake
        self.__logger.debug("ajouter_upload Deplacer repertoire upload vers %s" % path_intake)
        try:
            path_upload.rename(path_intake)
        except OSError as e:
            if e.errno == errno.ENOTEMPTY:
                self.__logger.info("ajouter_upload Repertoire intake pour %s existe deja (OK) - supprimer upload redondant" % fuuid)
                shutil.rmtree(path_upload)
                return
            else:
                raise e

        # Declencher traitement si pas deja en cours
        await self.trigger_traitement()

    async def emettre_transactions(self, job: IntakeJob):
        producer = self._etat_instance.producer
        await asyncio.wait_for(producer.producer_pret().wait(), 20)

        path_transaction = pathlib.Path(job.path_job, Constantes.FICHIER_TRANSACTION)
        try:
            with open(path_transaction, 'rb') as fichier:
                transaction = json.load(fichier)
        except FileNotFoundError:
            pass  # OK
        else:
            # Emettre transaction
            routage = transaction['routage']
            await producer.executer_commande(
                transaction,
                action=routage['action'], domaine=routage['domaine'], partition=routage.get('partition'),
                exchange=ConstantesMillegrille.SECURITE_PRIVE,
                timeout=60,
                noformat=True
            )

        path_cles = pathlib.Path(job.path_job, Constantes.FICHIER_CLES)
        try:
            with open(path_cles, 'rb') as fichier:
                cles = json.load(fichier)
        except FileNotFoundError:
            pass  # OK
        else:
            # Emettre transaction
            routage = cles['routage']
            producer.executer_commande(
                cles,
                action=routage['action'], domaine=routage['domaine'], partition=routage.get('partition'),
                exchange=ConstantesMillegrille.SECURITE_PRIVE,
                timeout=60,
                noformat=True
            )

    # def cleanup_download(self, fuuid):
    #     try:
    #         event_download = self.__events_fuuids[fuuid]
    #         del self.__events_fuuids[fuuid]
    #         event_download.set()
    #     except (AttributeError, KeyError) as e2:
    #         self.__logger.info("cleanup_download Erreur del info download %s en memoire (%s)" % (fuuid, e2))

    # async def __ajouter_job(self, info: InformationFuuid):
    #     """
    #     :param info: Fuuid a downloader et dechiffrer.
    #     :return:
    #     :raises asyncio.QueueFull: Si q de jobs est pleine.
    #     """
    #     fuuid = info.fuuid
    #
    #     try:
    #         self.__events_fuuids[fuuid]
    #     except KeyError:
    #         pass  # Ok, la job n'existe pas en memoire
    #     else:
    #         raise Exception("La job sur fuuid %s existe deja" % fuuid)
    #
    #     try:
    #         # Creer evenement d'attente pour metter les autres requetes en attente sur ce process
    #         self.__events_fuuids[fuuid] = asyncio.Event()
    #
    #         path_download_json = pathlib.Path(os.path.join(self.get_path_download(), fuuid + '.json'))
    #
    #         # Verifier que le fichier existe sur la consignation (requete HEAD)
    #         reponse_head = await self.__consignation_handler.verifier_existance(fuuid)
    #         status_fuuid = reponse_head['status']
    #         info_fichier = {
    #             'fuuid': fuuid,
    #             'mimetype': info.mimetype,
    #             'status': status_fuuid,
    #             'taille': reponse_head['taille'],
    #             'jwt_token': info.jwt_token
    #         }
    #         with path_download_json.open(mode='w') as fichier:
    #             json.dump(info_fichier, fichier)
    #
    #         if status_fuuid != 200:
    #             # Le fichier n'est pas disponible. Plus rien a faire
    #             self.__logger.debug('Fichier %s non disponible sur consignation' % fuuid)
    #             self.cleanup_download(fuuid)
    #             return info_fichier
    #     except Exception as e:
    #         self.__logger.exception("Erreur verification existance fichier %s" % fuuid)
    #         self.cleanup_download(fuuid)
    #         raise e
    #
    #     try:
    #         # Recuperer la cle pour dechiffrer la job
    #         ref_fuuid = info.ref or info.fuuid
    #         reponse_cle = None
    #         for i in range(1, CONST_MAX_RETRIES_CLE+1):
    #             self.__logger.debug("Recuperer_cle (try %d)" % i)
    #             try:
    #                 reponse_cle = await self.recuperer_cle(info.user_id, ref_fuuid, info.jwt_token, timeout=6)
    #                 break
    #             except (asyncio.CancelledError, asyncio.TimeoutError) as e:
    #                 self.__logger.warning("Timeout recuperer_cle (try %d de %d)" % (i, CONST_MAX_RETRIES_CLE))
    #                 if i == CONST_MAX_RETRIES_CLE:
    #                     raise e
    #
    #         cle_chiffree = reponse_cle['cle']
    #
    #         if info.format is None:
    #             # On travaille avec le fichier original, copier info chiffrage
    #             info.format = reponse_cle['format']
    #             info.header = reponse_cle.get('header')
    #
    #         self.__logger.debug('Creer la job de download pour fuuid %s' % fuuid)
    #         job = IntakeJob(info, cle_chiffree)
    #         self.__jobs.put_nowait(job)
    #         # S'assurer de demarrer le traitement immediatement
    #         await self.trigger_traitement()
    #     except Exception as e:
    #         # Set event attent et supprimer
    #         self.cleanup_download(fuuid)
    #
    #         # Cleanup du json, abort le download
    #         path_download_json.unlink(missing_ok=True)
    #
    #         raise e
    #
    #     return info_fichier


# def entretien_download(path_download: pathlib.Path, dict_attente: dict, timeout=Constantes.CONST_TIMEOUT_DOWNLOAD) -> list:
#     dt_expiration = datetime.datetime.now() - datetime.timedelta(seconds=timeout)
#     ts_expiration = dt_expiration.timestamp()
#
#     fuuids_supprimes = list()
#
#     # Supprimer les fichiers .dat (et .json associe)
#     for fichier in path_download.iterdir():
#         if fichier.match('*.work'):
#             fuuid = fichier.name.split('.')[0]
#             LOGGER.debug("Verifier expiration fichier download %s" % str(fichier))
#             stat_fichier = fichier.stat()
#             if stat_fichier.st_mtime < ts_expiration:
#                 LOGGER.debug("Download %s est expire, on le supprime" % fuuid)
#
#                 try:
#                     dict_attente[fuuid].set()
#                     del dict_attente[fuuid]
#                 except KeyError:
#                     pass  # OK
#
#                 fichier.unlink()
#
#                 fichier_json = pathlib.Path(str(fichier).replace('.work', '.json'))
#                 try:
#                     fichier_json.unlink()
#                 except FileNotFoundError:
#                     pass  # Ok
#                 fuuids_supprimes.append(fuuid)
#
#     # Cleanup des .json expires et orphelins
#     for fichier in path_download.iterdir():
#         if fichier.match('*.json'):
#             fuuid = fichier.name.split('.')[0]
#             stat_json = fichier.stat()
#             if stat_json.st_mtime < ts_expiration:
#                 # Le json est vieux - verifier s'il existe un fichier .work associe
#                 fichier_work = pathlib.Path(str(fichier).replace('.json', '.work'))
#                 if fichier_work.exists() is False:
#                     try:
#                         dict_attente[fuuid].set()
#                         del dict_attente[fuuid]
#                     except KeyError:
#                         pass  # OK
#
#                     # Le fichier json n'a aucun .dat associe, on supprime
#                     LOGGER.warning("Supprimer fichier json orphelin %s" % str(fichier))
#                     fichier.unlink()
#                     fuuids_supprimes.append(fuuid)
#
#     return fuuids_supprimes


# def entretien_dechiffre(path_dechiffre: pathlib.Path, timeout=Constantes.CONST_TIMEOUT_DECHIFFRE):
#     pass
#
#     """
#     Supprime les fichiers dechiffres qui ont ete supprimes.
#     :param path_dechiffre:
#     :param timeout:
#     :return: Liste de fuuids qui ont ete supprimes
#     """
#
#     dt_expiration = datetime.datetime.now() - datetime.timedelta(seconds=timeout)
#     ts_expiration = dt_expiration.timestamp()
#
#     fuuids_supprimes = list()
#
#     # Supprimer les fichiers .dat (et .json associe)
#     for fichier in path_dechiffre.iterdir():
#         if fichier.match('*.dat'):
#             LOGGER.debug("Verifier expiration fichier dechiffre %s" % str(fichier))
#             stat_fichier = fichier.stat()
#             if stat_fichier.st_mtime < ts_expiration:
#                 # Fichier expire
#                 LOGGER.debug("Fichier dechiffre est expire %s" % str(fichier))
#                 fuuid = fichier.name
#                 fichier.unlink()
#                 fichier_json = pathlib.Path(str(fichier).replace('.dat', '.json'))
#                 try:
#                     fichier_json.unlink()
#                 except FileNotFoundError:
#                     pass  # Ok
#                 fuuids_supprimes.append(fuuid)
#
#     # Cleanup des .json orphelins
#     for fichier in path_dechiffre.iterdir():
#         if fichier.match('*.json'):
#             fichier_dat = pathlib.Path(str(fichier).replace('.json', '.dat'))
#             if fichier_dat.exists() is False:
#                 LOGGER.warning("Supprimer fichier json orphelin %s" % str(fichier))
#                 # Le fichier json n'a aucun .dat associe, on supprime
#                 fichier.unlink()
#
#     return fuuids_supprimes

class RepertoireStat:

    def __init__(self, path_fichier: pathlib.Path):
        self.path_fichier = path_fichier
        self.stat = path_fichier.stat()

    @property
    def modification_date(self) -> float:
        return self.stat.st_mtime


def repertoires_par_date(path_parent: pathlib.Path) -> list[RepertoireStat]:

    repertoires = list()
    for item in path_parent.iterdir():
        if item.is_dir():
            repertoires.append(RepertoireStat(item))

    # Trier repertoires par date
    repertoires = sorted(repertoires, key=get_modification_date)

    return repertoires


def get_modification_date(item: RepertoireStat) -> float:
    return item.modification_date


def reassembler_fichier(job: IntakeJob) -> pathlib.Path:
    path_repertoire = job.path_job
    fuuid = job.fuuid
    path_fuuid = pathlib.Path(path_repertoire, fuuid)

    if path_fuuid.exists() is True:
        # Le fichier reassemble existe deja
        return path_fuuid

    path_work = pathlib.Path(path_repertoire, '%s.work' % fuuid)
    path_work.unlink(missing_ok=True)

    parts = sort_parts(path_repertoire)
    verificateur = VerificateurHachage(fuuid)
    with open(path_work, 'wb') as output:
        for position in parts:
            path_part = pathlib.Path(path_repertoire, '%d.part' % position)
            with open(path_part, 'rb') as part_file:
                while True:
                    chunk = part_file.read(64*1024)
                    if not chunk:
                        break
                    output.write(chunk)
                    verificateur.update(chunk)

    verificateur.verify()  # Lance ErreurHachage en cas de mismatch

    # Renommer le fichier .work
    path_work.rename(path_fuuid)

    return path_fuuid


def sort_parts(path_upload: pathlib.Path):
    positions = list()
    for item in path_upload.iterdir():
        if item.is_file():
            nom_fichier = str(item)
            if nom_fichier.endswith('.part'):
                position = int(item.name.split('.')[0])
                positions.append(position)
    positions = sorted(positions)
    return positions
