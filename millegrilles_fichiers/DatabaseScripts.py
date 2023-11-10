# Structure de la base de donnees

CONST_CREATE_FICHIERS = """

    CREATE TABLE IF NOT EXISTS fichiers(
        fuuid VARCHAR PRIMARY KEY,
        etat_fichier VARCHAR NOT NULL,
        taille INTEGER,
        bucket VARCHAR,
        date_presence TIMESTAMP,
        date_reclamation TIMESTAMP,
        date_verification TIMESTAMP,
        date_backup TIMESTAMP
    );
    
    CREATE INDEX IF NOT EXISTS fichiers_date_backup ON fichiers(date_backup ASC, etat_fichier);
    CREATE INDEX IF NOT EXISTS fichiers_date_verification ON fichiers(date_verification ASC, etat_fichier);
    CREATE INDEX IF NOT EXISTS fichiers_date_reclamation ON fichiers(date_reclamation ASC, etat_fichier);
        
"""

CONST_CREATE_SYNC = """
    CREATE TABLE fichiers(
        fuuid VARCHAR PRIMARY KEY,
        etat_fichier VARCHAR,
        taille INTEGER,
        bucket_reclame VARCHAR,
        date_reclamation TIMESTAMP,
        bucket_visite VARCHAR,
        date_presence TIMESTAMP
    );
    
    CREATE TABLE fichiers_primaire(
        fuuid VARCHAR PRIMARY KEY,
        etat_fichier VARCHAR NOT NULL,
        taille INTEGER,
        bucket VARCHAR
    );

    CREATE TABLE backups_primaire(
        uuid_backup VARCHAR NOT NULL,
        domaine VARCHAR NOT NULL,
        nom_fichier VARCHAR NOT NULL,
        taille INTEGER NOT NULL,
        PRIMARY KEY (uuid_backup, domaine, nom_fichier)
    );
"""

CONST_CREATE_BACKUP = """
    CREATE TABLE IF NOT EXISTS fichiers(
        fuuid VARCHAR PRIMARY KEY,
        taille INTEGER,
        date_backup TIMESTAMP
    );
"""

CONST_CREATE_TRANSFERTS = """
    CREATE TABLE IF NOT EXISTS downloads(
        fuuid VARCHAR PRIMARY KEY,
        taille INTEGER,
        date_creation TIMESTAMP NOT NULL,
        date_activite TIMESTAMP,
        essais INTEGER NOT NULL,
        erreur INTEGER
    );

    CREATE TABLE IF NOT EXISTS uploads(
        fuuid VARCHAR PRIMARY KEY,
        taille INTEGER NOT NULL,
        date_creation TIMESTAMP NOT NULL,
        date_activite TIMESTAMP,
        position INTEGER NOT NULL,
        essais INTEGER NOT NULL,
        erreur INTEGER
    );
    
    CREATE TABLE IF NOT EXISTS backups_primaire(
        uuid_backup VARCHAR NOT NULL,
        domaine VARCHAR NOT NULL,
        nom_fichier VARCHAR NOT NULL,
        taille INTEGER NOT NULL,
        flag_download INTEGER,
        PRIMARY KEY (uuid_backup, domaine, nom_fichier)
    );
"""

# SELECTs


SELECT_ETAT_DOWNLOADS = """
    SELECT count(fuuid), sum(taille)
    FROM downloads
    WHERE erreur IS NULL; 
"""

SELECT_ETAT_UPLOADS = """
    SELECT count(fuuid), sum(taille)
    FROM uploads
    WHERE erreur IS NULL; 
"""

SELECT_FICHIER_PAR_FUUID = """
    SELECT fuuid, etat_fichier, taille, bucket
    FROM FICHIERS
    WHERE fuuid = :fuuid;
"""

SELECT_PRIMAIRE_PAR_FUUID = """
    SELECT fuuid, etat_fichier, taille, bucket
    FROM FICHIERS_PRIMAIRE
    WHERE fuuid = :fuuid;
"""

SELECT_BACKUP_PRIMAIRE = """
    SELECT uuid_backup, domaine, nom_fichier, taille
    FROM backups_primaire
    WHERE uuid_backup = :uuid_backup
      AND domaine = :domaine
      AND nom_fichier = :nom_fichier;
"""

SELECT_BACKUP_STORE_FICHIERS = """
    SELECT fuuid, taille, bucket
    FROM fichiers
    WHERE etat_fichier = 'actif'
      AND taille IS NOT NULL
      AND (date_backup IS NULL OR date_backup < :date_sync) 
    LIMIT :limit;
"""

# INSERTs/UPDATEs

INSERT_SECONDAIRES_MANQUANTS = """
    INSERT OR IGNORE INTO FICHIERS(fuuid, etat_fichier, taille, bucket, date_presence, date_verification, date_reclamation)
    SELECT fuuid, 'manquant', taille, bucket, :date_reclamation, :date_reclamation, :date_reclamation
    FROM FICHIERS_PRIMAIRE;
"""

UPDATE_SECONDAIRES_ORPHELINS_VERS_ACTIF = """
    UPDATE FICHIERS
        SET etat_fichier = 'actif'
        WHERE etat_fichier = 'orphelin'
          AND fuuid IN (
            SELECT fuuid FROM FICHIERS_PRIMAIRE
        );
"""

UPDATE_SECONDAIRES_RECLAMES = """
    UPDATE FICHIERS
        SET date_reclamation = :date_reclamation
        WHERE fuuid IN (
            SELECT fuuid FROM FICHIERS_PRIMAIRE
        );
"""

UPDATE_SECONDAIRES_NON_RECLAMES_VERS_ORPHELINS = """
    UPDATE FICHIERS
        SET etat_fichier = 'orphelin'
        WHERE date_reclamation < :date_reclamation;
"""

INSERT_DOWNLOAD = """
    INSERT INTO DOWNLOADS(fuuid, taille, date_creation, essais)
    VALUES (:fuuid, :taille, :date_creation, 0);
"""

INSERT_UPLOADS = """
    INSERT OR IGNORE INTO UPLOADS(fuuid, taille, date_creation, position, essais)
    SELECT fp.fuuid, f.taille, :date_creation, 0, 0
    FROM FICHIERS_PRIMAIRE fp
    LEFT JOIN FICHIERS f on fp.fuuid = f.fuuid
    WHERE fp.etat_fichier = 'manquant'
      AND f.etat_fichier = 'actif';
"""

INSERT_UPLOAD = """
    INSERT INTO UPLOADS(fuuid, taille, date_creation, position, essais)
    VALUES(:fuuid, :taille, :date_creation, 0, 0);
"""

UPDATE_GET_NEXT_DOWNLOAD = """
    UPDATE DOWNLOADS
    SET date_activite = :date_activite,
        essais = essais + 1
    WHERE date_activite IS NULL
    RETURNING fuuid, taille
    LIMIT 1;
"""

UPDATE_GET_NEXT_UPLOAD = """
    UPDATE UPLOADS
    SET date_activite = :date_activite,
        essais = essais + 1
    WHERE date_activite IS NULL
    RETURNING fuuid, taille
    LIMIT 1;
"""

UPDATE_TOUCH_DOWNLOAD = """
    UPDATE DOWNLOADS
    SET date_activite = :date_activite,
        erreur = :erreur
    WHERE fuuid = :fuuid;
"""

DELETE_DOWNLOAD = """
    DELETE FROM DOWNLOADS
    WHERE fuuid = :fuuid;
"""

DELETE_DOWNLOADS_ESSAIS_EXCESSIFS = """
    DELETE FROM DOWNLOADS
    WHERE essais > 10;
"""

UPDATE_RESET_DOWNLOAD_EXPIRE = """
    UPDATE DOWNLOADS
    SET date_activite = null,
        erreur = null
    WHERE date_activite < :date_activite;
"""

UPDATE_RESET_UPLOADS_EXPIRE = """
    UPDATE UPLOADS
    SET date_activite = null,
        erreur = null
    WHERE date_activite < :date_activite;
"""

UPDATE_TOUCH_UPLOAD = """
    UPDATE UPLOADS
    SET date_activite = :date_activite,
        erreur = :erreur
    WHERE fuuid = :fuuid;
"""

INSERT_BACKUP_FICHIER = """
    INSERT INTO fichiers(fuuid, taille, date_backup)
    VALUES (:fuuid, :taille, :date_backup)
    ON CONFLICT(fuuid) DO UPDATE SET
       taille = :taille,
       date_backup = :date_backup;
"""

UPDATE_TOUCH_BACKUP_FICHIER = """
    UPDATE FICHIERS
    SET date_backup = :date_backup
    WHERE fuuid = :fuuid
      AND taille = :taille;
"""

DELETE_UPLOAD = """
    DELETE FROM UPLOADS
    WHERE fuuid = :fuuid;
"""

INSERT_FICHIER = """
    INSERT INTO FICHIERS(fuuid, etat_fichier, taille, bucket, date_presence, date_verification, date_reclamation)
    VALUES(:fuuid, :etat_fichier, :taille, :bucket, :date_presence, :date_verification, :date_reclamation);
"""

INSERT_OR_UPDATE_FICHIER = """
    INSERT INTO FICHIERS(fuuid, etat_fichier, taille, bucket, date_presence, date_verification, date_reclamation)
    VALUES(:fuuid, :etat_fichier, :taille, :bucket, :date_presence, :date_verification, :date_reclamation)
    ON CONFLICT(fuuid) DO UPDATE SET 
        etat_fichier = 'actif',
        taille = :taille,
        bucket = :bucket,
        date_presence = :date_presence,
        date_verification = :date_verification,
        date_reclamation = :date_reclamation
    ;
"""

UPDATE_ACTIVER_SI_MANQUANT = """
    UPDATE FICHIERS
    SET etat_fichier = :etat_fichier,
        bucket = :bucket
    WHERE fuuid = :fuuid AND etat_fichier = 'manquant'
"""

UPDATE_VERIFIER_FICHIER = """
    UPDATE FICHIERS
    SET taille = :taille, 
        date_presence = :date_verification, 
        date_verification = :date_verification
    WHERE fuuid = :fuuid
"""

INSERT_PRESENCE_FICHIERS = """
    INSERT INTO FICHIERS(fuuid, etat_fichier, taille, bucket_visite, date_presence)
    VALUES (:fuuid, 'actif', :taille, :bucket, :date_presence)
    ON CONFLICT(fuuid) DO UPDATE
    SET taille = :taille, 
        date_presence = :date_presence,
        bucket_visite = :bucket;
"""

INSERT_RECLAMER_FICHIER = """
    INSERT INTO FICHIERS(fuuid, bucket_reclame, date_reclamation)
    VALUES (:fuuid, :bucket, :date_reclamation)
"""

INSERT_RECLAMER_FICHIER_SECONDAIRE = """
    INSERT INTO FICHIERS(fuuid, bucket_reclame, date_reclamation, taille, etat_fichier)
    VALUES (:fuuid, :bucket, :date_reclamation, :taille, :etat_fichier)
    ON CONFLICT(fuuid) DO UPDATE SET
        bucket_reclame = :bucket,
        date_reclamation = :date_reclamation
    ;
"""

SELECT_STATS_FICHIERS = """
    SELECT etat_fichier, bucket, count(fuuid) as nombre, sum(taille) as taille
    FROM fichiers
    WHERE taille IS NOT NULL
    GROUP BY etat_fichier, bucket;
"""

SELECT_INFO_FICHIER = """
    SELECT fuuid, etat_fichier, taille, bucket, date_presence, date_verification, date_reclamation
    FROM fichiers 
    WHERE fuuid = :fuuid;
"""

SELECT_INFO_FICHIERS = """
    SELECT fuuid, etat_fichier, taille, bucket, bucket_visite, date_presence, date_verification, date_reclamation
    FROM fichiers 
    WHERE fuuid IN ($fuuids);
"""

SELECT_INFO_FICHIERS_ACTIFS = """
    SELECT fuuid, taille, bucket
    FROM fichiers 
    WHERE fuuid IN ($fuuids)
      AND etat_fichier = 'actif';
"""

UPDATE_MARQUER_ORPHELINS = """
    UPDATE FICHIERS
    SET etat_fichier = 'orphelin'
    WHERE date_reclamation < :date_reclamation
"""

UPDATE_MARQUER_ACTIF = """
    UPDATE FICHIERS
    SET etat_fichier = 'actif'
    WHERE date_reclamation >= :date_reclamation
      AND etat_fichier = 'orphelin'
"""

UPDATE_ACTIVER_SI_ORPHELIN = """
    UPDATE FICHIERS
    SET etat_fichier = 'actif',
        date_reclamation = :date_reclamation
    WHERE fuuid IN ($fuuids) 
      AND etat_fichier = 'orphelin'
"""

SELECT_BATCH_VERIFIER = """
    SELECT fuuid, taille, bucket
    FROM FICHIERS
    WHERE date_verification < :expiration_verification
      AND etat_fichier = 'actif'
      AND taille is NOT NULL
      AND bucket is NOT NULL
    ORDER BY date_verification
    LIMIT :limit;
"""

UPDATE_DATE_VERIFICATION = """
    UPDATE FICHIERS
    SET date_verification = :date_verification
    WHERE fuuid = :fuuid;
"""

UPDATE_DATE_ETATFICHIER = """
    UPDATE FICHIERS
    SET etat_fichier = :etat_fichier
    WHERE fuuid = :fuuid;
"""

UPDATE_DATE_ETATFICHIER_PRESENCE = """
    UPDATE FICHIERS
    SET etat_fichier = :etat_fichier,
        date_verification = :date_verification,
        date_presence = :date_presence,
        bucket_visite = NULL
    WHERE fuuid = :fuuid;
"""

UPDATE_ACTIFS_VISITES = """
    UPDATE FICHIERS
    SET etat_fichier = 'actif'
    WHERE date_presence >= :date_presence
      AND etat_fichier = 'manquant';
"""

UPDATE_MANQANTS_VISITES = """
    UPDATE FICHIERS
    SET etat_fichier = 'manquant'
    WHERE date_presence < :date_presence
      AND etat_fichier IN ('actif', 'orphelin');
"""

SELECT_BATCH_ORPHELINS = """
    SELECT fuuid, taille, bucket
    FROM FICHIERS
    WHERE date_reclamation < :date_reclamation
      AND etat_fichier IN ('orphelin', 'manquant')
      AND bucket is NOT NULL
    ORDER BY date_reclamation
    LIMIT :limit;
"""

DELETE_SUPPRIMER_FUUIDS = """
    DELETE FROM FICHIERS
    WHERE fuuid = :fuuid;
"""

SELECT_FICHIERS_TRANSFERT = """
    SELECT fuuid, etat_fichier, taille, bucket
    FROM FICHIERS
    WHERE etat_fichier IN ('actif', 'manquant');
"""

INSERT_FICHIER_PRIMAIRE = """
    INSERT OR REPLACE INTO FICHIERS_PRIMAIRE(fuuid, etat_fichier, taille, bucket)
    VALUES(:fuuid, :etat_fichier, :taille, :bucket);
"""

INSERT_BACKUP_PRIMAIRE = """
    INSERT OR REPLACE INTO BACKUPS_PRIMAIRE(uuid_backup, domaine, nom_fichier, taille)
    VALUES(:uuid_backup, :domaine, :nom_fichier, :taille);
"""

UPDATE_FETCH_BACKUP_PRIMAIRE = """
    UPDATE BACKUPS_PRIMAIRE
    SET flag_download = 1
    WHERE flag_download is NULL
    RETURNING uuid_backup, domaine, nom_fichier, taille
    LIMIT 10;
"""

# Autres

DELETE_TRUNCATE_FICHIERS_PRIMAIRE = """
    DELETE FROM FICHIERS_PRIMAIRE;
"""

DELETE_TRUNCATE_BACKUPS_PRIMAIRE = """
    DELETE FROM BACKUPS_PRIMAIRE;
"""


# Transferts

#         fuuid VARCHAR PRIMARY KEY,
#         etat_fichier VARCHAR NOT NULL,
#         taille INTEGER,
#         bucket VARCHAR NOT NULL,
#         date_presence TIMESTAMP NOT NULL,
#         date_reclamation TIMESTAMP,
#         date_verification TIMESTAMP,
#         date_backup TIMESTAMP

TRANSFERT_INSERT_PRESENCE_FICHIERS = """
    INSERT INTO fichiers.FICHIERS(fuuid, etat_fichier, taille, bucket, date_presence, date_reclamation)
    SELECT fuuid, 'actif', taille, bucket_visite, date_presence, date_reclamation
    FROM fichiers
    WHERE bucket_visite IS NOT NULL
    ON CONFLICT(fuuid) DO UPDATE SET 
        etat_fichier = 'actif',
        taille = excluded.taille,
        bucket = excluded.bucket,
        date_presence = excluded.date_presence,
        date_reclamation = excluded.date_reclamation,
        date_backup = excluded.date_backup
    ;
"""

TRANSFERT_INSERT_MANQUANTS_FICHIERS = """
    INSERT INTO fichiers.FICHIERS(fuuid, etat_fichier, date_reclamation)
    SELECT fuuid, 'manquant', date_reclamation
    FROM fichiers
    WHERE bucket_visite IS NULL
    ON CONFLICT(fuuid) DO UPDATE SET 
        etat_fichier = 'manquant',
        date_reclamation = excluded.date_reclamation
    ;
"""

TRANSFERT_INSERT_ORPHELINS_FICHIERS = """
    INSERT INTO fichiers.FICHIERS(fuuid, etat_fichier, date_presence)
    SELECT fuuid, 'orphelin', date_presence
    FROM fichiers
    WHERE bucket_reclame IS NULL
    ON CONFLICT(fuuid) DO UPDATE SET 
        etat_fichier = 'orphelin',
        date_presence = excluded.date_presence
    ;
"""

TRANSFERT_UPDATE_BACKUPS = """
    UPDATE destination.fichiers as dest
    SET date_backup = source.date_backup
    FROM (SELECT fuuid, taille, date_backup FROM fichiers) as source
    WHERE dest.fuuid = source.fuuid
      AND dest.taille = source.taille
    ;
"""

TRANSFERT_INSERT_DOWNLOADS = """
    INSERT OR IGNORE INTO transferts.downloads(fuuid, taille, date_creation, essais)
    SELECT fuuid, taille, :date_creation, 0
    FROM fichiers
    WHERE etat_fichier = 'actif'
      AND date_presence IS NULL
      AND date_reclamation IS NOT NULL
    ;
"""

TRANSFERT_INSERT_UPLOADS = """
    INSERT OR IGNORE INTO transferts.uploads(fuuid, taille, date_creation, position, essais)
    SELECT fmain.fuuid, fmain.taille, :date_creation, 0, 0
    FROM fichiers fsync
    LEFT JOIN fichiers.fichiers fmain on fsync.fuuid = fmain.fuuid
    WHERE fsync.etat_fichier = 'manquant'
      AND fmain.etat_fichier = 'actif'
    ;
"""

TRANSFERT_INSERT_BACKUPS = """
INSERT OR IGNORE INTO transferts.backups_primaire(uuid_backup, domaine, nom_fichier, taille)
SELECT uuid_backup, domaine, nom_fichier, taille
FROM backups_primaire
;
"""

TRANSFERT_UPDATE_MARQUER_ORPHELINS = """
    UPDATE fichiers.fichiers
    SET etat_fichier = 'orphelin'
    WHERE date_reclamation < :date_reclamation
"""
