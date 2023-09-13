# Structure de la base de donnees

CONST_CREATE_FICHIERS = """
    CREATE TABLE IF NOT EXISTS FICHIERS(
        fuuid VARCHAR PRIMARY KEY,
        etat_fichier VARCHAR NOT NULL,
        taille INTEGER,
        bucket VARCHAR,
        bucket_visite VARCHAR,
        date_presence TIMESTAMP NOT NULL,
        date_verification TIMESTAMP NOT NULL,
        date_reclamation TIMESTAMP NOT NULL     
    );

    CREATE TABLE IF NOT EXISTS FICHIERS_PRIMAIRE(
        fuuid VARCHAR PRIMARY KEY,
        etat_fichier VARCHAR NOT NULL,
        taille INTEGER,
        bucket VARCHAR
    );
    
    CREATE TABLE IF NOT EXISTS DOWNLOADS(
        fuuid VARCHAR PRIMARY KEY,
        taille INTEGER,
        date_creation TIMESTAMP NOT NULL,
        date_activite TIMESTAMP,
        essais INTEGER NOT NULL,
        erreur INTEGER
    );

    CREATE TABLE IF NOT EXISTS UPLOADS(
        fuuid VARCHAR PRIMARY KEY,
        taille INTEGER NOT NULL,
        date_creation TIMESTAMP NOT NULL,
        date_activite TIMESTAMP,
        position INTEGER NOT NULL,
        essais INTEGER NOT NULL,
        erreur INTEGER
    );

    CREATE TABLE IF NOT EXISTS BACKUPS_PRIMAIRE(
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
    FROM DOWNLOADS
    WHERE erreur is null; 
"""

SELECT_ETAT_UPLOADS = """
    SELECT count(fuuid), sum(taille)
    FROM UPLOADS
    WHERE erreur is null; 
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

# INSERTs/UPDATEs

COMMANDE_INSERT_SECONDAIRES_MANQUANTS = """
    INSERT OR IGNORE INTO FICHIERS(fuuid, etat_fichier, taille, bucket, date_presence, date_verification, date_reclamation)
    SELECT fuuid, 'manquant', taille, bucket, :date_reclamation, :date_reclamation, :date_reclamation
    FROM FICHIERS_PRIMAIRE
    WHERE etat_fichier = 'actif';
"""

COMMANDE_UPDATE_SECONDAIRES_ORPHELINS_VERS_ACTIF = """
    UPDATE FICHIERS
        SET etat_fichier = 'actif'
        WHERE etat_fichier = 'orphelin'
          AND fuuid IN (
            SELECT fuuid FROM FICHIERS_PRIMAIRE
        );
"""

COMMANDE_UPDATE_SECONDAIRES_RECLAMES = """
    UPDATE FICHIERS
        SET date_reclamation = :date_reclamation
        WHERE fuuid IN (
            SELECT fuuid FROM FICHIERS_PRIMAIRE
        );
"""

COMMANDE_UPDATE_SECONDAIRES_NON_RECLAMES_VERS_ORPHELINS = """
    UPDATE FICHIERS
        SET etat_fichier = 'orphelin'
        WHERE date_reclamation < :date_reclamation;
"""

COMMAND_INSERT_DOWNLOADS = """
    INSERT OR IGNORE INTO DOWNLOADS(fuuid, taille, date_creation, essais)
    SELECT fuuid, taille, :date_creation, 0
    FROM FICHIERS
    WHERE etat_fichier = 'manquant';
"""

COMMAND_INSERT_DOWNLOAD = """
    INSERT INTO DOWNLOADS(fuuid, taille, date_creation, essais)
    VALUES (:fuuid, :taille, :date_creation, 0);
"""

COMMAND_INSERT_UPLOADS = """
    INSERT OR IGNORE INTO UPLOADS(fuuid, taille, date_creation, position, essais)
    SELECT p.fuuid, f.taille, :date_creation, 0, 0
    FROM FICHIERS_PRIMAIRE p, FICHIERS f
    WHERE p.fuuid = f.fuuid
      AND f.etat_fichier = 'actif'
      AND p.etat_fichier = 'manquant';
"""

COMMAND_INSERT_UPLOAD = """
    INSERT INTO UPLOADS(fuuid, taille, date_creation, position, essais)
    VALUES(:fuuid, :taille, :date_creation, 0, 0);
"""

COMMANDE_GET_NEXT_DOWNLOAD = """
    UPDATE DOWNLOADS
    SET date_activite = :date_activite,
        essais = essais + 1
    WHERE date_activite IS NULL
    RETURNING fuuid, taille
    LIMIT 1;
"""

COMMANDE_GET_NEXT_UPLOAD = """
    UPDATE UPLOADS
    SET date_activite = :date_activite,
        essais = essais + 1
    WHERE date_activite IS NULL
    RETURNING fuuid, taille
    LIMIT 1;
"""

COMMANDE_TOUCH_DOWNLOAD = """
    UPDATE DOWNLOADS
    SET date_activite = :date_activite,
        erreur = :erreur
    WHERE fuuid = :fuuid;
"""

COMMANDE_DELETE_DOWNLOAD = """
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

COMMANDE_TOUCH_UPLOAD = """
    UPDATE UPLOADS
    SET date_activite = :date_activite,
        erreur = :erreur
    WHERE fuuid = :fuuid;
"""

COMMANDE_DELETE_UPLOAD = """
    DELETE FROM UPLOADS
    WHERE fuuid = :fuuid;
"""

CONST_INSERT_FICHIER = """
    INSERT INTO FICHIERS(fuuid, etat_fichier, taille, bucket, date_presence, date_verification, date_reclamation)
    VALUES(:fuuid, :etat_fichier, :taille, :bucket, :date_presence, :date_verification, :date_reclamation);
"""

CONST_ACTIVER_SI_MANQUANT = """
    UPDATE FICHIERS
    SET etat_fichier = :etat_fichier,
        bucket = :bucket
    WHERE fuuid = :fuuid AND etat_fichier = 'manquant'
"""

CONST_VERIFIER_FICHIER = """
    UPDATE FICHIERS
    SET taille = :taille, 
        date_presence = :date_verification, 
        date_verification = :date_verification
    WHERE fuuid = :fuuid
"""

CONST_PRESENCE_FICHIERS = """
    INSERT INTO FICHIERS(fuuid, etat_fichier, taille, bucket, bucket_visite, date_presence, date_verification, date_reclamation)
    VALUES (:fuuid, :etat_fichier, :taille, :bucket, :bucket, :date_presence, :date_presence, :date_presence)
    ON CONFLICT(fuuid) DO UPDATE
    SET taille = :taille, 
        date_presence = :date_presence,
        bucket_visite = :bucket;
"""

CONST_RECLAMER_FICHIER = """
    INSERT INTO FICHIERS(fuuid, etat_fichier, bucket, date_presence, date_verification, date_reclamation)
    VALUES (:fuuid, :etat_fichier, :bucket, :date_reclamation, :date_reclamation, :date_reclamation)
    ON CONFLICT(fuuid) DO UPDATE
    SET bucket = :bucket,
        date_reclamation = :date_reclamation;
"""

CONST_STATS_FICHIERS = """
    SELECT etat_fichier, bucket, count(fuuid) as nombre, sum(taille) as taille
    FROM fichiers
    GROUP BY etat_fichier, bucket;
"""

CONST_INFO_FICHIER = """
    SELECT fuuid, etat_fichier, taille, bucket, date_presence, date_verification, date_reclamation
    FROM fichiers 
    WHERE fuuid = :fuuid;
"""

CONST_INFO_FICHIERS = """
    SELECT fuuid, etat_fichier, taille, bucket, bucket_visite, date_presence, date_verification, date_reclamation
    FROM fichiers 
    WHERE fuuid IN ($fuuids);
"""

CONST_INFO_FICHIERS_ACTIFS = """
    SELECT fuuid, taille, bucket
    FROM fichiers 
    WHERE fuuid IN ($fuuids)
      AND etat_fichier = 'actif';
"""

CONST_MARQUER_ORPHELINS = """
    UPDATE FICHIERS
    SET etat_fichier = 'orphelin'
    WHERE date_reclamation < :date_reclamation
"""

CONST_MARQUER_ACTIF = """
    UPDATE FICHIERS
    SET etat_fichier = 'actif'
    WHERE date_reclamation >= :date_reclamation
      AND etat_fichier = 'orphelin'
"""

CONST_ACTIVER_SI_ORPHELIN = """
    UPDATE FICHIERS
    SET etat_fichier = 'actif',
        date_reclamation = :date_reclamation
    WHERE fuuid IN ($fuuids) 
      AND etat_fichier = 'orphelin'
"""

REQUETE_BATCH_VERIFIER = """
    SELECT fuuid, taille, bucket_visite
    FROM FICHIERS
    WHERE date_verification < :expiration_verification
      AND etat_fichier = 'actif'
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

REQUETE_BATCH_ORPHELINS = """
    SELECT fuuid, taille, bucket_visite
    FROM FICHIERS
    WHERE date_reclamation < :date_reclamation
      AND etat_fichier IN ('orphelin', 'manquant')
    ORDER BY date_reclamation
    LIMIT :limit;
"""

DELETE_SUPPRIMER_FUUIDS = """
    DELETE FROM FICHIERS
    WHERE fuuid = :fuuid;
"""

REQUETE_FICHIERS_TRANSFERT = """
    SELECT fuuid, etat_fichier, taille, bucket
    FROM FICHIERS
    WHERE etat_fichier IN ('actif', 'manquant');
"""

COMMANDE_INSERT_FICHIER_PRIMAIRE = """
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

COMMANDE_TRUNCATE_FICHIERS_PRIMAIRE = """
    DELETE FROM FICHIERS_PRIMAIRE;
"""

COMMANDE_TRUNCATE_BACKUPS_PRIMAIRE = """
    DELETE FROM BACKUPS_PRIMAIRE;
"""
