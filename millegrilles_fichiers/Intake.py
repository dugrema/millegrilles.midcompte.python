import asyncio
import errno
import json
import logging
import pathlib
import shutil
import sqlite3

from typing import Optional

from millegrilles_messages.messages import Constantes as ConstantesMillegrille
from millegrilles_messages.messages.Hachage import VerificateurHachage, ErreurHachage
from millegrilles_messages.messages.MessagesModule import MessageWrapper
from millegrilles_messages.jobs.Intake import IntakeHandler
from millegrilles_messages.MilleGrillesConnecteur import EtatInstance
from millegrilles_messages.FileLocking import FileLock, FileLockedException, is_locked

from millegrilles_fichiers import Constantes
from millegrilles_fichiers.Consignation import ConsignationHandler, StoreNonInitialise

LOGGER = logging.getLogger(__name__)

CONST_INTAKE_LOCK_NAME = 'intake.lock'
CONST_CHUNK_SIZE = 64 * 1024


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

        self.__path_intake = pathlib.Path(self._etat_instance.configuration.dir_staging, Constantes.DIR_STAGING_INTAKE)
        self.__path_intake.mkdir(parents=True, exist_ok=True)

    def get_path_intake_fuuid(self, fuuid: str):
        return pathlib.Path(self.__path_intake, fuuid)

    async def run(self):
        await asyncio.gather(
            super().run(),
            # self.trigger_regulier(),
            # self.entretien_dechiffre_thread(),
            # self.entretien_download_thread()
        )

    # async def trigger_regulier(self):
    #     # Declenchement initial du traitement (recovery)
    #     try:
    #         await asyncio.wait_for(self._stop_event.wait(), timeout=5)
    #     except asyncio.TimeoutError:
    #         pass  # OK
    #
    #     while self._stop_event.is_set() is False:
    #         if self.__consignation_handler.sync_en_cours is False:
    #             await self.trigger_traitement()
    #         try:
    #             await asyncio.wait_for(self._stop_event.wait(), timeout=300)
    #         except asyncio.TimeoutError:
    #             pass  # OK

    async def trigger_cedule(self, trigger: MessageWrapper):
        # cedule_heure = datetime.datetime.fromtimestamp(trigger.parsed['estampille'], tz=pytz.UTC)
        # cedule_minute = cedule_heure.minute

        # if cedule_minute % 5 == 0:  # Redemarrer aux 5 minutes
        if self._stop_event.is_set() is False:
            if self.__consignation_handler.sync_en_cours is False:
                await self.trigger_traitement()

    async def configurer(self):
        return await super().configurer()

    async def traiter_prochaine_job(self) -> Optional[dict]:
        if self.__consignation_handler.sync_en_cours:
            self.__logger.info("traiter_prochaine_job Sync en cours, on ignore traitement de jobs")
            return None

        try:
            await asyncio.wait_for(self.__consignation_handler.store_pret_wait(), 20)
        except asyncio.TimeoutError:
            self.__logger.warning("traiter_prochaine_job Store n'est pas pret, on arrete le traitement")
            return None

        try:
            repertoires = repertoires_par_date(self.__path_intake)
            path_repertoire = repertoires[0].path_fichier
            fuuid = path_repertoire.name
            repertoires = None
            path_lock = pathlib.Path(path_repertoire, CONST_INTAKE_LOCK_NAME)
            with FileLock(path_lock, lock_timeout=300):
                self.__logger.debug("traiter_prochaine_job Traiter job intake fichier pour fuuid %s" % fuuid)
                path_repertoire.touch()  # Touch pour mettre a la fin en cas de probleme de traitement
                job = IntakeJob(fuuid, path_repertoire)
                await self.traiter_job(job)
        except IndexError:
            return None  # Condition d'arret de l'intake
        except FileLockedException:
            return {'ok': False, 'err': 'job locked - traitement en cours'}
        except FileNotFoundError as e:
            raise e  # Erreur fatale
        except Exception as e:
            self.__logger.exception("traiter_prochaine_job Erreur traitement job download")
            return {'ok': False, 'err': str(e)}

        return {'ok': True}

    async def annuler_job(self, job: dict, emettre_evenement=False):
        raise NotImplementedError('must override')

    async def traiter_job(self, job):
        # Reassembler et valider le fichier
        path_consignation = self._etat_instance.configuration.dir_consignation
        path_staging_consignation = pathlib.Path(path_consignation, Constantes.DIR_STAGING_INTAKE)

        # Verifier si le fichier existe deja (e.g. retry pour transaction)
        info_retry = await self.charger_fichier_retry(job)
        if info_retry.get('consigne') is not True:
            try:
                args = [job]
                argv = {'work_path': path_staging_consignation}
                path_fichier = await asyncio.to_thread(reassembler_fichier, *args, **argv)
            except ErreurHachage as e:
                self.__logger.exception("traiter_job Erreur hachage pour le fichier reassemble %s" % job.fuuid)
                await self.handle_retries(job, reject=True)  # Rejeter immediatement (vers repertoire reject)
                raise e
            except FileNotFoundError:
                # Aucunes parts a reassembler,
                self.__logger.warning("Aucunes parts a reassembler, assumer fichier vide pour %s" % job.fuuid)
            except Exception as e:
                self.__logger.exception("traiter_job Erreur reassemblage fichier %s" % job.fuuid)
                await self.handle_retries(job)  # Incrementer le nombre de retries
                raise e
            else:
                # Incrementer le nombre de retries
                # Noter que le fichier est reassemble, valide et transfere vers le repertoire de consignation
                await self.handle_retries(job)

            # Consigner : deplacer fichier vers repertoire final
            limit_retry = 5
            consignation_ok = False
            for i in range(0, limit_retry):  # 5 essais, 5 a 10 secondes chaque
                try:
                    await self.__consignation_handler.consigner(path_fichier, job.fuuid)
                    consignation_ok = True
                    break  # Ok
                except sqlite3.OperationalError as erreur:
                    self.__logger.info(
                        "Erreur store DB locked sur intake - attendre 5 secondes et reessayer")
                    if i == limit_retry - 1:
                        raise erreur
                except StoreNonInitialise as erreur:
                    self.__logger.info("Erreur store non initialise sur intake - attendre 5 secondes et reessayer")
                    if i == limit_retry - 1:
                        raise erreur
                await asyncio.sleep(5)

            if consignation_ok is not True:
                raise Exception('Echec consignation fichier %s' % job.fuuid)

        # Charger transactions/cles. Emettre.
        try:
            await self.emettre_transactions(job)
        except Exception as e:
            # Marquer consignation fichier OK, retry sera juste pour la transaction
            await self.marquer_contenu_consigne(job)
            raise e

        if self._etat_instance.est_primaire is False:
            self.__logger.debug("traiter_job Fichier consigne sur secondaire - s'assurer que le fichier existe sur primaire")
            try:
                await self.__consignation_handler.ajouter_upload_secondaire(job.fuuid)
            except:
                self.__logger.exception("traiter_job Erreur ajout upload secondaire")

        # Supprimer le repertoire de la job
        self.__logger.info("Job completee, suppression repertoire %s" % job.fuuid)
        shutil.rmtree(job.path_job)

    async def charger_fichier_retry(self, job) -> dict:
        # Marque le fichier comme etant pret sous la consignation
        path_repertoire = job.path_job
        path_fichier_retry = pathlib.Path(path_repertoire, 'retry.json')

        try:
            with open(path_fichier_retry, 'rt') as fichier:
                info_retry = json.load(fichier)
        except FileNotFoundError:
            info_retry = {}

        return info_retry

    async def marquer_contenu_consigne(self, job):
        # Marque le fichier comme etant pret sous la consignation
        path_repertoire = job.path_job
        path_fichier_retry = pathlib.Path(path_repertoire, 'retry.json')

        try:
            with open(path_fichier_retry, 'rt') as fichier:
                info_retry = json.load(fichier)
        except FileNotFoundError:
            info_retry = {}

        # Reset retries : le fichier est bon et on attend de soumettre la transasction (incluant la cle)
        info_retry['retry'] = -1
        info_retry['consigne'] = True

        with open(path_fichier_retry, 'wt') as fichier:
            json.dump(info_retry, fichier)

    async def handle_retries(self, job: IntakeJob, reject=False):
        path_repertoire = job.path_job
        fuuid = job.fuuid
        path_fichier_retry = pathlib.Path(path_repertoire, 'retry.json')

        if self.__consignation_handler.sync_en_cours:
            self.__logger.info("Skip retry %s, sync en cours" % job.fuuid)
            return

        # Conserver marqueur pour les retries en cas d'erreur
        try:
            with open(path_fichier_retry, 'rt') as fichier:
                info_retry = json.load(fichier)
        except FileNotFoundError:
            info_retry = {'retry': -1}

        if reject is True or info_retry['retry'] > 5:
            self.__logger.error("Job %s irrecuperable, trop de retries" % fuuid)
            # shutil.rmtree(path_repertoire)
            path_rejected = pathlib.Path(path_repertoire.parent.parent, 'rejected')
            path_rejected.mkdir(exist_ok=True)
            path_rejected_fuuid = pathlib.Path(path_rejected, path_repertoire.name)
            try:
                path_repertoire.rename(path_rejected_fuuid)
            except OSError as e:
                if e.errno == errno.ENOTEMPTY:
                    # Erreur de transfert vers retry, directory existe deja. On remplace
                    shutil.rmtree(path_rejected_fuuid)
                    path_repertoire.rename(path_rejected_fuuid)
                else:
                    self.__logger.exception("Erreur transfert job vers rejected, suppression de l'original")
                    shutil.rmtree(path_repertoire)
            if reject is False:
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
        fuuid = job.fuuid
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
            resultat_commande = await producer.executer_commande(
                transaction,
                action=routage['action'], domaine=routage['domaine'], partition=routage.get('partition'),
                exchange=ConstantesMillegrille.SECURITE_PRIVE,
                timeout=60,
                noformat=True
            )
            if resultat_commande.parsed.get('ok') is False:
                self.__logger.error("Erreur sauvegarder transaction fichier : %s" % resultat_commande.parsed)
                raise ErreurTraitementTransaction(resultat_commande.parsed)

        path_cles = pathlib.Path(job.path_job, Constantes.FICHIER_CLES)
        try:
            with open(path_cles, 'rb') as fichier:
                cles = json.load(fichier)
        except FileNotFoundError:
            pass  # OK
        else:
            # Emettre transaction
            routage = cles['routage']
            resultat_cle = await producer.executer_commande(
                cles,
                action=routage['action'], domaine=routage['domaine'], partition=routage.get('partition'),
                exchange=ConstantesMillegrille.SECURITE_PUBLIC,
                timeout=60,
                noformat=True
            )
            if resultat_cle.parsed.get('ok') is False:
                raise ErreurTraitementTransaction(resultat_cle.parsed)

        # Re-emettre l'evenement de visite. Si le fichier n'etait pas deja initialise, l'evenement a ete perdu.
        await self.__consignation_handler.emettre_evenement_consigne(fuuid)


class ErreurTraitementTransaction(Exception):

    def __init__(self, message:dict, *args):
        super().__init__(*args)
        self.message = message


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
            path_lock = pathlib.Path(item, CONST_INTAKE_LOCK_NAME)
            if is_locked(path_lock, timeout=300) is False:
                repertoires.append(RepertoireStat(item))

    # Trier repertoires par date
    repertoires = sorted(repertoires, key=get_modification_date)

    return repertoires


def get_modification_date(item: RepertoireStat) -> float:
    return item.modification_date


def reassembler_fichier(job: IntakeJob, work_path: Optional[pathlib.Path] = None) -> pathlib.Path:
    path_repertoire = job.path_job
    fuuid = job.fuuid

    if work_path is not None:
        path_workfile = pathlib.Path(work_path, '%s.work' % fuuid)
        path_fuuid = pathlib.Path(work_path, fuuid)
    else:
        path_workfile = pathlib.Path(path_repertoire, '%s.work' % fuuid)
        path_fuuid = pathlib.Path(path_repertoire, fuuid)

    if path_fuuid.exists() is True:
        # Le fichier reassemble existe deja
        LOGGER.debug("reassembler_fichier Existe deja (OK) : %s" % path_fuuid)
        return path_fuuid

    path_workfile.unlink(missing_ok=True)

    LOGGER.info("reassembler_fichier Path : %s" % path_fuuid)

    parts = sort_parts(path_repertoire)

    if len(parts) == 0:
        raise FileNotFoundError()  # On n'a aucunes parts a reassembler

    verificateur = VerificateurHachage(fuuid)
    with open(path_workfile, 'wb') as output:
        for position in parts:
            path_part = pathlib.Path(path_repertoire, '%d.part' % position)
            with open(path_part, 'rb') as part_file:
                while True:
                    chunk = part_file.read(CONST_CHUNK_SIZE)
                    if not chunk:
                        break
                    output.write(chunk)
                    verificateur.update(chunk)

    verificateur.verify()  # Lance ErreurHachage en cas de mismatch

    # Renommer le fichier .work
    path_workfile.rename(path_fuuid)

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
