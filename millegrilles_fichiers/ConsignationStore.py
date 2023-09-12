import asyncio
import datetime
import errno
import gzip
import json
import logging
import pathlib
import shutil
import sqlite3
import tempfile

from aiohttp import web
from typing import Optional, Type

import pytz

from millegrilles_messages.messages import Constantes as ConstantesMillegrilles
from millegrilles_messages.messages.Hachage import VerificateurHachage, ErreurHachage

import millegrilles_fichiers.DatabaseScripts as scripts_database
from millegrilles_fichiers import Constantes
from millegrilles_fichiers.EtatFichiers import EtatFichiers


class EntretienDatabase:

    def __init__(self, etat: EtatFichiers, check_same_thread=True):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self._etat = etat

        path_database = pathlib.Path(
            self._etat.configuration.dir_consignation, Constantes.DIR_DATA, Constantes.FICHIER_DATABASE)
        self.__path_database = path_database

        self.__con = sqlite3.connect(self.__path_database, check_same_thread=check_same_thread)
        self.__cur = self.__con.cursor()

        self.__batch_visites: Optional[dict] = None

        self.__limite_batch = 100

        self.__debut_entretien = datetime.datetime.now(tz=pytz.UTC)

    def ajouter_visite(self, bucket: str, fuuid: str, taille: int):
        if self.__batch_visites is None:
            self.__batch_visites = list()
        row = {
            'fuuid': fuuid,
            'etat_fichier': Constantes.DATABASE_ETAT_ACTIF,
            'taille': taille,
            'bucket': bucket,
            'date_presence': datetime.datetime.now(tz=pytz.UTC),
        }
        self.__batch_visites.append(row)

        if len(self.__batch_visites) >= self.__limite_batch:
            self.commit_visites()

    def commit_visites(self):
        batch = self.__batch_visites
        self.__batch_visites = None
        if batch is not None:
            self.__cur.executemany(scripts_database.CONST_PRESENCE_FICHIERS, batch)
        self.__con.commit()

    def marquer_actifs_visites(self):
        """ Sert a marquer tous les fichiers "manquants" comme actifs si visites recemment. """
        self.__cur.execute(scripts_database.UPDATE_ACTIFS_VISITES, {'date_presence': self.__debut_entretien})
        self.__cur.execute(scripts_database.UPDATE_MANQANTS_VISITES, {'date_presence': self.__debut_entretien})
        self.__con.commit()

    def marquer_verification(self, fuuid: str, etat_fichier: str):
        if etat_fichier == Constantes.DATABASE_ETAT_ACTIF:
            # Mettre a jour la date de verification
            params = {
                'fuuid': fuuid,
                'date_verification': datetime.datetime.now(tz=pytz.UTC)
            }
            self.__cur.execute(scripts_database.UPDATE_DATE_VERIFICATION, params)
        else:
            # Erreur - mettre a jour l'etat seulement
            params = {
                'fuuid': fuuid,
                'etat_fichier': etat_fichier
            }
            self.__cur.execute(scripts_database.UPDATE_DATE_ETATFICHIER, params)
        self.__con.commit()

    def identifier_orphelins(self, expiration: datetime.datetime) -> list:
        params = {
            'date_reclamation': expiration,
            'limit': 1000,
        }
        cur = self.__con.cursor()
        orphelins = list()
        try:
            cur.execute(scripts_database.REQUETE_BATCH_ORPHELINS, params)
            while True:
                row = cur.fetchone()
                if row is None:
                    break
                fuuid, taille, bucket_visite = row
                orphelins.append({'fuuid': fuuid, 'taille': taille, 'bucket': bucket_visite})
        finally:
            cur.close()

        return orphelins

    def supprimer(self, fuuid: str):
        params = {'fuuid': fuuid}
        cur = self.__con.cursor()
        try:
            cur.execute(scripts_database.DELETE_SUPPRIMER_FUUIDS, params)
        finally:
            self.__con.commit()
            cur.close()

    def generer_relamations_primaires(self, fp):
        """ Genere le contenu du fichier de transfert d'etat fichiers.jsonl.gz """
        cur = self.__con.cursor()
        try:
            cur.execute(scripts_database.REQUETE_FICHIERS_TRANSFERT)
            while True:
                row = cur.fetchone()
                if row is None:
                    break
                fuuid, etat_fichier, taille, bucket_visite = row
                contenu_ligne = {'fuuid': fuuid, 'etat_fichier': etat_fichier, 'taille': taille, 'bucket': bucket_visite}
                json.dump(contenu_ligne, fp)
                fp.write('\n')
        finally:
            cur.close()

        self.reset_intermediaires()

    def get_path_database(self):
        dir_data = pathlib.Path(self._etat.configuration.dir_consignation, Constantes.DIR_DATA)
        return pathlib.Path(dir_data, Constantes.FICHIER_DATABASE)

    def get_path_reclamations(self):
        dir_data = pathlib.Path(self._etat.configuration.dir_consignation, Constantes.DIR_DATA)
        return pathlib.Path(dir_data, Constantes.FICHIER_RECLAMATIONS_PRIMAIRES)

    def get_path_relamations_intermediaires(self):
        dir_data = pathlib.Path(self._etat.configuration.dir_consignation, Constantes.DIR_DATA)
        return pathlib.Path(dir_data, Constantes.FICHIER_RECLAMATIONS_INTERMEDIAIRES)

    def reset_intermediaires(self):
        path_intermediaires = self.get_path_relamations_intermediaires()
        path_intermediaires.unlink(missing_ok=True)

    def consigner(self, fuuid: str, taille: int, bucket: str):
        date_now = datetime.datetime.now(tz=pytz.UTC)
        data = {
            'fuuid': fuuid,
            'etat_fichier': Constantes.DATABASE_ETAT_ACTIF,
            'taille': taille,
            'bucket': bucket,
            'date_presence': date_now,
            'date_verification': date_now,
            'date_reclamation': date_now,
        }

        nouveau = False

        try:
            self.__cur.execute(scripts_database.CONST_INSERT_FICHIER, data)
            nouveau = True
        except sqlite3.IntegrityError as e:
            if 'FICHIERS.fuuid' in e.args[0]:
                self.__logger.debug("ConsignationStore.consigner fuuid %s existe deja - OK" % fuuid)
                self.__cur.execute(scripts_database.CONST_ACTIVER_SI_MANQUANT, data)
                self.__cur.execute(scripts_database.CONST_VERIFIER_FICHIER, data)
            else:
                raise e
        finally:
            self.__con.commit()

        if nouveau:
            try:
                data_intermediaire = {
                    'fuuid': fuuid,
                    'etat_fichier': Constantes.DATABASE_ETAT_ACTIF,
                    'taille': taille,
                    'bucket': bucket,
                }
                path_intermediaire = self.get_path_relamations_intermediaires()
                with open(path_intermediaire, 'at') as fichier:
                    json.dump(data_intermediaire, fichier)
                    fichier.write('\n')
            except Exception:
                self.__logger.exception("Erreur sauvegarde fuuid dans liste intermediaire")

    def truncate_fichiers_primaire(self):
        self.__cur.execute(scripts_database.COMMANDE_TRUNCATE_FICHIERS_PRIMAIRE)
        self.__con.commit()

    def ajouter_fichier_primaire(self, fichier: dict):
        if self.__batch_visites is None:
            self.__batch_visites = list()
        self.__batch_visites.append(fichier)

        if len(self.__batch_visites) >= self.__limite_batch:
            self.commit_fichiers_primaire()

    def commit_fichiers_primaire(self):
        batch = self.__batch_visites
        self.__batch_visites = None
        if batch is not None:
            self.__cur.executemany(scripts_database.COMMANDE_INSERT_FICHIER_PRIMAIRE, batch)
        self.__con.commit()

    def marquer_secondaires_reclames(self):
        date_debut = datetime.datetime.now(tz=pytz.UTC)

        # Inserer secondaires manquants
        params = {'date_reclamation': datetime.datetime.now(tz=pytz.UTC)}
        self.__cur.execute(scripts_database.COMMANDE_INSERT_SECONDAIRES_MANQUANTS, params)
        self.__con.commit()

        # Convertir les secondaires orphelins en actifs
        self.__cur.execute(scripts_database.COMMANDE_UPDATE_SECONDAIRES_ORPHELINS_VERS_ACTIF, params)
        self.__con.commit()

        # Marquer les secondaires deja presents comme reclames (peu importe l'etat)
        params = {'date_reclamation': datetime.datetime.now(tz=pytz.UTC)}
        self.__cur.execute(scripts_database.COMMANDE_UPDATE_SECONDAIRES_RECLAMES, params)
        self.__con.commit()

        params = {'date_reclamation': date_debut}
        self.__cur.execute(scripts_database.COMMANDE_UPDATE_SECONDAIRES_NON_RECLAMES_VERS_ORPHELINS, params)
        self.__con.commit()

    def generer_downloads(self):
        params = {'date_creation': datetime.datetime.now(tz=pytz.UTC)}
        self.__cur.execute(scripts_database.COMMAND_INSERT_DOWNLOADS, params)
        self.__con.commit()

    def generer_uploads(self):
        params = {'date_creation': datetime.datetime.now(tz=pytz.UTC)}
        self.__cur.execute(scripts_database.COMMAND_INSERT_UPLOADS, params)
        self.__con.commit()

    def get_next_download(self):
        params = {'date_activite': datetime.datetime.now(tz=pytz.UTC)}
        self.__cur.execute(scripts_database.COMMANDE_GET_NEXT_DOWNLOAD, params)
        row = self.__cur.fetchone()
        self.__con.commit()
        if row is not None:
            fuuid, taille = row
            return {'fuuid': fuuid, 'taille': taille}
        return None

    def get_next_upload(self):
        params = {'date_creation': datetime.datetime.now(tz=pytz.UTC)}
        raise NotImplementedError('todo')
        # self.__cur.execute(scripts_database.COMMANDE_GET_NEXT_UPLOAD, params)
        # row = self.__cur.fetchone()
        # self.__con.commit()
        # if row is not None:
        #     fuuid, taille = row
        #     return {'fuuid': fuuid, 'taille': taille}
        # return None

    def touch_download(self, fuuid: str, erreur: Optional[int] = None):
        params = {'fuuid': fuuid, 'date_activite': datetime.datetime.now(tz=pytz.UTC), 'erreur': erreur}
        self.__cur.execute(scripts_database.COMMANDE_TOUCH_DOWNLOAD, params)
        self.__con.commit()

    def get_etat_downloads(self):
        self.__cur.execute(scripts_database.SELECT_ETAT_DOWNLOADS)
        cur = self.__con.cursor()
        try:
            row = self.__cur.fetchone()
            if row:
                nombre, taille = row
                return {'nombre': nombre, 'taille': taille}
        finally:
            cur.close()

        return None

    def entretien_transferts(self):
        """ Marque downloads ou uploads expires, permet nouvel essai. """
        pass

    def supprimer_job_download(self, fuuid: str):
        params = {'fuuid': fuuid}
        self.__cur.execute(scripts_database.COMMANDE_DELETE_DOWNLOAD, params)
        self.__con.commit()

    def close(self):
        self.__con.commit()
        self.__cur.close()
        self.__con.close()


class ConsignationStore:

    def __init__(self, etat: EtatFichiers):
        self._etat = etat
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

        # Creer repertoire data, path database
        path_database = pathlib.Path(
            self._etat.configuration.dir_consignation, Constantes.DIR_DATA, Constantes.FICHIER_DATABASE)
        self.__path_database = path_database

        self._stop_store: Optional[asyncio.Event] = None

    def initialiser(self):
        self._stop_store = asyncio.Event()

        path_backup = pathlib.Path(
            self._etat.configuration.dir_consignation, Constantes.DIR_BACKUP)
        path_backup.mkdir(parents=True, exist_ok=True)

        self.__path_database.parent.mkdir(parents=True, exist_ok=True)
        con = self.ouvrir_database()
        con.executescript(scripts_database.CONST_CREATE_FICHIERS)
        con.close()

    async def run_entretien(self):
        raise NotImplementedError('must override')

    async def stop(self):
        self._stop_store.set()

    def get_path_fuuid(self, bucket: str, fuuid: str) -> pathlib.Path:
        raise NotImplementedError('must override')

    def __consigner_db(self, path_src: pathlib.Path, fuuid: str):
        stat = path_src.stat()
        entretien_db = EntretienDatabase(self._etat)
        try:
            entretien_db.consigner(fuuid, stat.st_size, Constantes.BUCKET_PRINCIPAL)
        finally:
            entretien_db.close()

    async def consigner(self, path_src: pathlib.Path, fuuid: str):
        # Tenter d'inserer le fichier comme nouveau actif dans la base de donnees
        await asyncio.to_thread(self.__consigner_db, path_src, fuuid)

        # con = sqlite3.connect(self.__path_database, check_same_thread=True)
        # stat = path_src.stat()
        #
        # date_now = datetime.datetime.now(tz=pytz.UTC)
        # data = {
        #     'fuuid': fuuid,
        #     'etat_fichier': Constantes.DATABASE_ETAT_ACTIF,
        #     'taille': stat.st_size,
        #     'bucket': Constantes.BUCKET_PRINCIPAL,
        #     'date_presence': date_now,
        #     'date_verification': date_now,
        #     'date_reclamation': date_now,
        # }
        # cur = con.cursor()
        # try:
        #     cur.execute(scripts_database.CONST_INSERT_FICHIER, data)
        # except sqlite3.IntegrityError as e:
        #     if 'FICHIERS.fuuid' in e.args[0]:
        #         self.__logger.debug("ConsignationStore.consigner fuuid %s existe deja - OK" % fuuid)
        #         cur.execute(scripts_database.CONST_ACTIVER_SI_MANQUANT, data)
        #         cur.execute(scripts_database.CONST_VERIFIER_FICHIER, data)
        #     else:
        #         raise e
        # finally:
        #     cur.close()
        #     con.commit()
        #     con.close()

        # await self.emettre_batch_visites([fuuid], verification=True)
        await self.emettre_evenement_consigne(fuuid)

    async def changer_bucket(self, fuuid: str, bucket: str):
        """ Deplace le fichier vers un bucket """
        raise NotImplementedError('must override')

    async def supprimer(self, bucket: str, fuuid: str):
        """ Marquer orphelin si fichier est actif """
        raise NotImplementedError('must override')

    async def recuperer(self, fuuid: str):
        """ Marquer le fichier orphelin comme actif """
        raise NotImplementedError('must override')

    async def purger(self, fuuid: str):
        """ Retirer le fichier du bucket """
        raise NotImplementedError('must override')

    async def stream_fuuid(self, fuuid: str, response: web.StreamResponse, start: Optional[int] = None, end: Optional[int] = None):
        """ Stream les bytes du fichier en utilisant la response """
        raise NotImplementedError('must override')

    async def entretien(self):
        """ Declenche un entretien (visite, verification, purge, etc.) """
        raise NotImplementedError('must override')

    async def visiter_fuuids(self):
        """ Visiter tous les fichiers presents, s'assurer qu'ils sont dans la base de donnees. """
        raise NotImplementedError('must override')

    async def verifier_fuuids(self, limite=Constantes.CONST_LIMITE_TAILLE_VERIFICATION):
        await asyncio.to_thread(self.__verifier_fuuids, limite)

    def __verifier_fuuids(self, limite=Constantes.CONST_LIMITE_TAILLE_VERIFICATION):
        """ Visiter tous les fichiers presents, s'assurer qu'ils sont dans la base de donnees. """
        liste_fichiers = self.__charger_verifier_fuuids(limite)

        entretien_db = EntretienDatabase(self._etat)

        # Verifier chaque fichier individuellement
        try:
            for fichier in liste_fichiers:
                self.verifier_fichier(entretien_db, **fichier)
        finally:
            entretien_db.close()

    async def supprimer_orphelins(self):
        producer = self._etat.producer
        await asyncio.wait_for(producer.producer_pret().wait(), timeout=10)

        est_primaire = self._etat.est_primaire
        if est_primaire:
            expiration_secs = Constantes.CONST_EXPIRATION_ORPHELIN_PRIMAIRE
        else:
            expiration_secs = Constantes.CONST_EXPIRATION_ORPHELIN_SECONDAIRE
        expiration = datetime.datetime.now(tz=pytz.UTC) - datetime.timedelta(seconds=expiration_secs)
        self.__logger.info("supprimer_orphelins Expires depuis %s" % expiration)

        entretien_db = EntretienDatabase(self._etat, check_same_thread=False)

        try:
            batch_orphelins = await asyncio.to_thread(entretien_db.identifier_orphelins, expiration)
            fuuids = [f['fuuid'] for f in batch_orphelins]
            fuuids_a_supprimer = set(fuuids)
            if est_primaire:
                # Transmettre commande pour confirmer que la suppression peut avoir lieu
                commande = {'fuuids': fuuids}
                reponse = await producer.executer_commande(
                    commande,
                    domaine=Constantes.DOMAINE_GROSFICHIERS, action=Constantes.COMMANDE_SUPPRIMER_ORPHELINS,
                    exchange=ConstantesMillegrilles.SECURITE_PRIVE, timeout=30
                )
                if reponse.parsed['ok'] is True:
                    # Retirer tous les fuuids a conserver de la liste a supprimer
                    fuuids_conserver = reponse.parsed.get('fuuids_a_conserver') or list()
                    fuuids_a_supprimer = fuuids_a_supprimer.difference(fuuids_conserver)

            for fichier in batch_orphelins:
                fuuid = fichier['fuuid']
                bucket = fichier['bucket']
                if fuuid in fuuids_a_supprimer:
                    self.__logger.info("supprimer_orphelins Supprimer fichier orphelin expire %s" % fichier)
                    try:
                        await self.supprimer(bucket, fuuid)
                    except OSError as e:
                        if e.errno == errno.ENOENT:
                            self.__logger.warning("Erreur suppression fichier %s/%s - non trouve" % (bucket, fuuid))
                        else:
                            raise e

                    # Supprimer de la base de donnes locale
                    await asyncio.to_thread(entretien_db.supprimer, fuuid)
        finally:
            entretien_db.close()

        pass

    def verifier_fichier(self, entretien_db: EntretienDatabase, fuuid: str, taille: int, bucket: str):
        """ Verifie un fichier individuellement. Marque resultat dans la base de donnees. """
        raise NotImplementedError('must override')

    async def conserver_backup(self, fichier_temp: tempfile.TemporaryFile, uuid_backup: str, domaine: str,
                               nom_fichier: str):
        raise NotImplementedError('must override')

    async def rotation_backups(self, uuid_backups_conserver: list[str]):
        """ Supprime tous les backups qui ne sont pas dans la liste """
        raise NotImplementedError('must override')

    async def get_stat_fichier(self, fuuid: str, bucket: Optional[str] = None) -> Optional[dict]:
        """
        Retourne de l'information de base d'un fichier
        :param fuuid:
        :param bucket:
        :return: dict ou None
        """
        raise NotImplementedError('must override')

    async def reactiver_fuuids(self, commande: dict):
        """ Reactiver un fuuid (orphelin -> actif) si applicable. Retourne """
        fuuids = commande['fuuids']
        sur_echec = commande.get('sur_echec') or False

        fuuids_actifs = await asyncio.to_thread(self.__reactiver_fuuids, fuuids)

        if len(fuuids_actifs) == 0 and sur_echec is False:
            return  # Aucun message de retour

        # Verifier l'existence des fichiers
        fuuids_inconnus = set(fuuids)
        fuuids_trouves = list()
        erreurs = list()
        for fuuid in fuuids_actifs:
            fuuids_inconnus.remove(fuuid)  # Retirer de la liste inconnu - va etre actif ou erreur
            # Acceder au fichier sur disque
            info = await self.get_stat_fichier(fuuid)
            if info is not None:
                fuuids_trouves.append(fuuid)
            else:
                erreurs.append({'fuuid': fuuid, 'code': 1, 'err': 'FileNotFound'})

        reponse = {
            'recuperes': fuuids_actifs,
            'inconnus': list(fuuids_inconnus),
            'errors': erreurs,
        }

        return reponse

    def __reactiver_fuuids(self, fuuids: list) -> list[str]:
        con = self.ouvrir_database()
        cur = con.cursor()

        dict_fuuids = dict()
        idx = 0
        for fuuid in fuuids:
            dict_fuuids['f%d' % idx] = fuuid
            idx += 1

        params = {
            'date_reclamation': datetime.datetime.now(tz=pytz.UTC)
        }
        params.update(dict_fuuids)

        requete = scripts_database.CONST_ACTIVER_SI_ORPHELIN.replace('$fuuids', ','.join([':%s' % f for f in dict_fuuids.keys()]))

        cur.execute(requete, params)
        con.commit()

        liste_fichiers_actifs = list()
        requete = scripts_database.CONST_INFO_FICHIERS_ACTIFS.replace('$fuuids', ','.join([':%s' % f for f in dict_fuuids.keys()]))
        cur.execute(requete, dict_fuuids)

        while True:
            row = cur.fetchone()
            if row is None:
                break
            fuuid = row[0]
            liste_fichiers_actifs.append(fuuid)

        cur.close()
        con.close()

        return liste_fichiers_actifs

    def ouvrir_database(self, ro=False) -> sqlite3.Connection:
        return sqlite3.connect(self.__path_database, check_same_thread=True)

    def get_stats(self):
        con = self.ouvrir_database()
        cur = con.cursor()
        cur.execute(scripts_database.CONST_STATS_FICHIERS)

        resultats_dict = dict()
        nombre_orphelins = 0
        taille_orphelins = 0
        nombre_manquants = 0

        while True:
            row = cur.fetchone()
            if row is None:
                break

            etat_fichier, bucket, nombre, taille = row
            if etat_fichier == Constantes.DATABASE_ETAT_ACTIF:
                resultats_dict[bucket] = {
                    'nombre': nombre,
                    'taille': taille
                }
            elif etat_fichier == Constantes.DATABASE_ETAT_ORPHELIN:
                nombre_orphelins += nombre
                taille_orphelins += taille
            elif etat_fichier == Constantes.DATABASE_ETAT_MANQUANT:
                nombre_manquants += nombre

        cur.close()
        con.close()

        resultats_dict[Constantes.DATABASE_ETAT_ORPHELIN] = {
            'nombre': nombre_orphelins,
            'taille': taille_orphelins
        }

        resultats_dict[Constantes.DATABASE_ETAT_MANQUANT] = {
            'nombre': nombre_manquants,
        }

        return resultats_dict

    def get_info_fichier(self, fuuid: str):
        con = self.ouvrir_database()
        cur = con.cursor()
        cur.execute(scripts_database.CONST_INFO_FICHIER, {'fuuid': fuuid})
        _fuuid, etat_fichier, taille, bucket, date_presence, date_verification, date_reclamation = cur.fetchone()
        cur.close()
        con.close()

        return {
            'fuuid': fuuid,
            'taille': taille,
            'etat_fichier': etat_fichier,
            'date_presence': date_presence,
            'date_verification': date_verification,
            'date_reclamation': date_reclamation
        }

    async def emettre_batch_visites(self, fuuids: list[str], verification=False):
        producer = self._etat.producer
        await asyncio.wait_for(producer.producer_pret().wait(), timeout=10)

        message = {
            'fuuids': fuuids,
            'verification': verification,
        }
        await producer.emettre_evenement(
            message,
            domaine=Constantes.DOMAINE_FICHIERS, action=Constantes.EVENEMENT_VISITER_FUUIDS,
            exchanges=ConstantesMillegrilles.SECURITE_PRIVE
        )

    async def emettre_evenement_consigne(self, fuuid: str):
        producer = self._etat.producer
        await asyncio.wait_for(producer.producer_pret().wait(), timeout=10)

        message = {'hachage_bytes': fuuid}
        await producer.emettre_evenement(
            message,
            domaine=Constantes.DOMAINE_FICHIERS, action=Constantes.EVENEMENT_FICHIER_CONSIGNE,
            exchanges=ConstantesMillegrilles.SECURITE_PRIVE
        )

        if self._etat.est_primaire:
            message = {'fuuid': fuuid}
            await producer.emettre_evenement(
                message,
                domaine=Constantes.DOMAINE_FICHIERS, action=Constantes.EVENEMENT_CONSIGNATION_PRIMAIRE,
                exchanges=ConstantesMillegrilles.SECURITE_PRIVE
            )

    async def reclamer_fuuids_database(self, fuuids: list, bucket: str):
        await asyncio.to_thread(self.__reclamer_fuuids_database, fuuids, bucket)

    def __reclamer_fuuids_database(self, fuuids: list, bucket: str):
        rows = list()
        for fuuid in fuuids:
            rows.append({
                'fuuid': fuuid,
                'etat_fichier': Constantes.DATABASE_ETAT_MANQUANT,
                'bucket': bucket,
                'date_reclamation': datetime.datetime.now(tz=pytz.UTC)
            })

        con = self.ouvrir_database()
        cur = con.cursor()
        cur.executemany(scripts_database.CONST_RECLAMER_FICHIER, rows)
        con.commit()
        cur.close()
        con.close()

    async def marquer_orphelins(self, debut_reclamation: datetime.datetime, complet=False):
        await asyncio.to_thread(self.__marquer_orphelins, debut_reclamation, complet)

    def __marquer_orphelins(self, debut_reclamation: datetime.datetime, complet=False):
        con = self.ouvrir_database()
        cur = con.cursor()

        if complet:
            # Marquer les fichiers avec vieille date de reclamation comme non reclames (orphelins)
            cur.execute(scripts_database.CONST_MARQUER_ORPHELINS, {'date_reclamation': debut_reclamation})

        # Marquer fichiers orphelins qui viennent d'etre reclames comme actif
        cur.execute(scripts_database.CONST_MARQUER_ACTIF, {'date_reclamation': debut_reclamation})

        con.commit()
        cur.close()
        con.close()

    def __charger_verifier_fuuids(self, limite_taille: Constantes.CONST_LIMITE_TAILLE_VERIFICATION) -> list[dict]:
        # Generer une batch de fuuids a verifier
        limite_nombre = 1000

        # La reverification permet de controler la frequence de verification d'un fichier (e.g. aux trois mois)
        expiration = datetime.datetime.now(tz=pytz.UTC) - datetime.timedelta(seconds=Constantes.CONST_INTERVALLE_REVERIFICATION)
        params = {
            'expiration_verification': expiration,
            'limit': limite_nombre
        }

        con = self.ouvrir_database()
        cur = con.cursor()
        cur.execute(scripts_database.REQUETE_BATCH_VERIFIER, params)

        taille_totale = 0
        fuuids = list()
        while True:
            row = cur.fetchone()
            if row is None:
                break
            fuuid, taille, bucket_visite = row
            taille_totale += taille
            if len(fuuids) > 0 and taille_totale > limite_taille:
                break  # On a atteint la limite en bytes
            fuuids.append({'fuuid': fuuid, 'taille': taille, 'bucket': bucket_visite})

        cur.close()
        con.close()

        return fuuids

    async def generer_reclamations_sync(self):
        dir_data = pathlib.Path(self._etat.configuration.dir_consignation)
        fichier_reclamations = pathlib.Path(dir_data,
                                            Constantes.DIR_DATA, Constantes.FICHIER_RECLAMATIONS_PRIMAIRES)

        # Preparer le fichier work
        fichier_reclamations_work = pathlib.Path('%s.work' % fichier_reclamations)
        fichier_reclamations_work.unlink(missing_ok=True)

        entretien_db = EntretienDatabase(self._etat, check_same_thread=False)
        try:
            with gzip.open(fichier_reclamations_work, 'wt') as fichier:
                await asyncio.to_thread(entretien_db.generer_relamations_primaires, fichier)

            # Renommer fichier .work pour remplacer le fichier de reclamations precedent
            fichier_reclamations.unlink(missing_ok=True)
            fichier_reclamations_work.rename(fichier_reclamations)
        except:
            self.__logger.exception('Erreur generation fichier reclamations')
            fichier_reclamations_work.unlink(missing_ok=True)
        finally:
            entretien_db.close()


class ConsignationStoreMillegrille(ConsignationStore):

    def __init__(self, etat: EtatFichiers):
        super().__init__(etat)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

    def get_path_fuuid(self, bucket: str, fuuid: str) -> pathlib.Path:
        dir_consignation = pathlib.Path(self._etat.configuration.dir_consignation)
        sub_folder = fuuid[-2:]
        path_fuuid = pathlib.Path(dir_consignation, Constantes.DIR_BUCKETS, bucket, sub_folder, fuuid)
        return path_fuuid

    async def consigner(self, path_src: pathlib.Path, fuuid: str):
        await super().consigner(path_src, fuuid)

        path_dest = self.get_path_fuuid(Constantes.BUCKET_PRINCIPAL, fuuid)
        # Tenter de deplacer avec rename
        try:
            path_dest.parent.mkdir(parents=True, exist_ok=True)
            path_src.rename(path_dest)
        except OSError as e:
            if e.errno == errno.EEXIST:
                self.__logger.info(
                    "ConsignationStoreMillegrille.consigner Le fuuid %s existe deja - supprimer la source" % fuuid)
                path_src.unlink()
            else:
                raise e

    # async def thread_visiter(self):
    #     stop_coro = self._stop_store.wait()
    #     await asyncio.wait([stop_coro], timeout=30)
    #     while self._stop_store.is_set() is False:
    #         self.__logger.info("thread_visiter Debut visiter fuuids")
    #         try:
    #             await asyncio.to_thread(self.visiter_fuuids)
    #         except Exception:
    #             self.__logger.exception('thread_visiter Erreur visite fuuids')
    #         await asyncio.wait([stop_coro], timeout=Constantes.CONST_INTERVALLE_VISITE_MILLEGRILLE)

    async def run_entretien(self):
        pass

    async def changer_bucket(self, fuuid: str, bucket: str):
        raise NotImplementedError('todo')

    async def supprimer(self, bucket: str, fuuid: str):
        path_fichier = self.get_path_fuuid(bucket, fuuid)
        path_fichier.unlink()

    async def purger(self, fuuid: str):
        raise NotImplementedError('todo')

    async def stream_fuuid(
            self, fuuid: str, response: web.StreamResponse,
            start: Optional[int] = None, end: Optional[int] = None
    ):
        # Pour local FS, ignore la base de donnes. On verifie si le fichier existe dans actif ou archives
        path_fichier = self.get_path_fuuid(Constantes.BUCKET_PRINCIPAL, fuuid)
        if path_fichier.exists() is False:
            path_fichier = self.get_path_fuuid(Constantes.BUCKET_ARCHIVES, fuuid)
            if path_fichier.exists() is False:
                raise Exception('fichier inconnu %s' % fuuid)

        with path_fichier.open(mode='rb') as input_file:
            if start is not None and start > 0:
                input_file.seek(start, 0)
                position = start
            else:
                position = 0
            for chunk in input_file:
                if end is not None and position + len(chunk) > end:
                    taille_chunk = end - position + 1
                    await response.write(chunk[:taille_chunk])
                    break  # Termine
                else:
                    await response.write(chunk)
                position += len(chunk)

    async def visiter_fuuids(self):
        await asyncio.to_thread(self.__visiter_fuuids)

    def __visiter_fuuids(self):
        dir_buckets = pathlib.Path(self._etat.configuration.dir_consignation, Constantes.DIR_BUCKETS)
        entretien_db = EntretienDatabase(self._etat)

        try:
            # Parcourir tous les buckets recursivement (depth-first)
            for bucket in dir_buckets.iterdir():
                self.visiter_bucket(bucket.name, bucket, entretien_db)

            entretien_db.commit_visites()

            # Marquer tous les fichiers 'manquants' qui viennent d'etre visites comme actifs (utilise date debut)
            entretien_db.marquer_actifs_visites()
        finally:
            entretien_db.close()

    def visiter_bucket(self, bucket: str, path_repertoire: pathlib.Path, entretien_db: EntretienDatabase):
        for item in path_repertoire.iterdir():
            if item.is_dir():
                # Parcourir recursivement
                self.visiter_bucket(bucket, item, entretien_db)
            elif item.is_file():
                stat = item.stat()
                entretien_db.ajouter_visite(bucket, item.name, stat.st_size)

    async def conserver_backup(self, fichier_temp: tempfile.TemporaryFile, uuid_backup: str, domaine: str,
                               nom_fichier: str):
        path_backup = pathlib.Path(self._etat.configuration.dir_consignation, Constantes.DIR_BACKUP, uuid_backup, domaine)
        # Utiliser thread separee pour processus de copie (blocking)
        await asyncio.to_thread(self.__conserver_backup, fichier_temp, path_backup, nom_fichier)

    def __conserver_backup(self, fichier_temp: tempfile.TemporaryFile, repertoire: pathlib.Path, nom_fichier):
        fichier_temp.seek(0)
        path_fichier_backup = pathlib.Path(repertoire, nom_fichier)
        path_fichier_work = pathlib.Path(repertoire, '%s.work' % nom_fichier)
        repertoire.mkdir(parents=True, exist_ok=True)
        with open(path_fichier_work, 'wb') as fichier:
            while True:
                chunk = fichier_temp.read(64*1024)
                if not chunk:
                    break
                fichier.write(chunk)

        # Renommer fichier (retrirer .work)
        path_fichier_work.rename(path_fichier_backup)

    async def rotation_backups(self, uuid_backups_conserver: list[str]):
        """ Supprime tous les backups qui ne sont pas dans la liste """
        dir_consignation = pathlib.Path(self._etat.configuration.dir_consignation, Constantes.DIR_BACKUP)
        for item in dir_consignation.iterdir():
            if item.is_dir():
                if item.name not in uuid_backups_conserver:
                    self.__logger.info("Supprimer repertoire de backup %s" % item.name)
                    shutil.rmtree(item)

    async def get_stat_fichier(self, fuuid: str, bucket: Optional[str] = None) -> Optional[dict]:
        stat = None
        if bucket is None:
            path_fichier = self.get_path_fuuid(Constantes.BUCKET_PRINCIPAL, fuuid)
            try:
                stat = path_fichier.stat()
            except FileNotFoundError:
                path_fichier = self.get_path_fuuid(Constantes.BUCKET_ARCHIVES, fuuid)
                try:
                    stat = path_fichier.stat()
                except FileNotFoundError:
                    pass
        else:
            path_fichier = self.get_path_fuuid(bucket, fuuid)
            try:
                stat = path_fichier.stat()
            except FileNotFoundError:
                pass

        if stat is None:
            return None

        info = {
            'size': stat.st_size,
            'ctime': int(stat.st_ctime),
        }

        return info

    def verifier_fichier(self, entretien_db: EntretienDatabase, fuuid: str, taille: int, bucket: str):
        path_fichier = self.get_path_fuuid(bucket, fuuid)
        verificateur = VerificateurHachage(fuuid)

        try:
            with open(path_fichier, 'rb') as fichier:
                while True:
                    chunk = fichier.read(64*1024)
                    if not chunk:
                        break
                    verificateur.update(chunk)
            try:
                verificateur.verify()
                entretien_db.marquer_verification(fuuid, Constantes.DATABASE_ETAT_ACTIF)
            except ErreurHachage:
                self.__logger.error("verifier_fichier Fichier %s est corrompu, marquer manquant" % fuuid)
                entretien_db.marquer_verification(fuuid, Constantes.DATABASE_ETAT_MANQUANT)
                path_fichier.unlink()  # Supprimer le fichier invalide
        except FileNotFoundError:
            self.__logger.error("verifier_fichier Fichier %s est absent, marquer manquant" % fuuid)
            entretien_db.marquer_verification(fuuid, Constantes.DATABASE_ETAT_MANQUANT)


def map_type(type_store: str) -> Type[ConsignationStore]:
    if type_store == Constantes.TYPE_STORE_MILLEGRILLE:
        return ConsignationStoreMillegrille
    elif type_store == Constantes.TYPE_STORE_SFTP:
        raise NotImplementedError()
    elif type_store == Constantes.TYPE_STORE_AWSS3:
        raise NotImplementedError()
    else:
        raise Exception('Type %s non supporte' % type_store)
