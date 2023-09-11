PARAM_CERT_PATH = 'CERT_PEM'
PARAM_KEY_PATH = 'KEY_PEM'
PARAM_CA_PATH = 'CA_PATH'

ENV_WEB_PORT = 'WEB_PORT'

ENV_DIR_CONSIGNATION = 'DIR_CONSIGNATION'
ENV_PATH_KEY_SSH_ED25519 = 'SFTP_ED25519_KEY'
ENV_PATH_KEY_SSH_RSA = 'SFTP_RSA_KEY'

DIR_BUCKETS = 'buckets'
DIR_ACTIFS = 'actifs'
DIR_ARCHIVES = 'archives'
DIR_ORPHELINS = 'orphelins'
DIR_DATA = 'data'
DIR_STAGING_UPLOAD = 'staging/upload'
DIR_STAGING_INTAKE = 'staging/intake'
DIR_BACKUP = 'backup_transactions'

FICHIER_DATABASE = 'consignation.db'
FICHIER_RECLAMATIONS_PRIMAIRES = 'reclamations.jsonl.gz'
FICHIER_RECLAMATIONS_INTERMEDIAIRES = 'reclamations.txt'

FICHIER_ETAT = 'etat.json'
FICHIER_TRANSACTION = 'transaction.json'
FICHIER_CLES = 'cles.json'

DOMAINE_GROSFICHIERS = 'GrosFichiers'
DOMAINE_CORE_TOPOLOGIE = 'CoreTopologie'
DOMAINE_FICHIERS = 'fichiers'

QUEUE_PRIMAIRE_NOM = 'fichiers/primaire'

EVENEMENT_CEDULE = 'cedule'
EVENEMENT_PRESENCE = 'presence'
EVENEMENT_VISITER_FUUIDS = 'visiterFuuids'
EVENEMENT_FICHIER_CONSIGNE = 'consigne'
EVENEMENT_SYNC_PRIMAIRE = 'syncPrimaire'
EVENEMENT_SYNC_PRET = 'syncPret'
EVENEMENT_SYNC_SECONDAIRE = 'declencherSyncSecondaire'
EVENEMENT_CHANGEMENT_CONSIGNATION_PRIMAIRE = 'changementConsignationPrimaire'

COMMANDE_MODIFIER_CONFIGURATION = 'modifierConfiguration'
COMMANDE_ENTRETIEN_BACKUP = 'entretienBackup'
COMMANDE_DECLENCHER_SYNC = 'declencherSync'
COMMANDE_ACTIVITE_FUUIDS = 'confirmerActiviteFuuids'
COMMANDE_RECLAMER_FUUIDS = 'reclamerFuuids'
COMMANDE_REACTIVER_FUUIDS = 'reactiverFuuids'
COMMANDE_SUPPRIMER_ORPHELINS = 'supprimerOrphelins'

REQUETE_PUBLIC_KEY_SSH = 'getPublicKeySsh'
REQUETE_LISTE_DOMAINES = 'listeDomaines'
REQUETE_VERIFIER_EXISTANCE = 'verifierExistance'

CONST_CHAMPS_CONFIG = ['type_store', 'url_download', 'consignation_url']

TYPE_STORE_MILLEGRILLE = 'millegrille'
TYPE_STORE_SFTP = 'sftp'
TYPE_STORE_AWSS3 = 'awss3'

DATABASE_ETAT_ACTIF = 'actif'
# DATABASE_ETAT_ARCHIVE = 'archive'
DATABASE_ETAT_ORPHELIN = 'orphelin'
DATABASE_ETAT_MANQUANT = 'manquant'

BUCKET_PRINCIPAL = 'principal'
BUCKET_ARCHIVES = 'archives'

CONST_INTERVALLE_VISITE_MILLEGRILLE = 3600 * 8
CONST_INTERVALLE_VISITE_SFTP = 3600 * 24
CONST_INTERVALLE_VISITE_AWSS3 = 3600 * 72

CONST_VISITE_BATCH_LIMITE = 200

CONST_INTERVALLE_VERIFICATION = 60 * 20
CONST_INTERVALLE_REVERIFICATION = 86400 * 90
CONST_LIMITE_TAILLE_VERIFICATION = 1_000_000_000

CONST_DEFAUT_SYNC_INTERVALLE = 3600 * 8

CONST_EXPIRATION_ORPHELIN_PRIMAIRE = 86400 * 21
# CONST_EXPIRATION_ORPHELIN_PRIMAIRE = 120
CONST_EXPIRATION_ORPHELIN_SECONDAIRE = 86400 * 7

CONST_INTERVALLE_ORPHELINS = 3600
