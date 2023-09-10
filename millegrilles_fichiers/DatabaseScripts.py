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
    WHERE date_presence > :date_presence
      AND etat_fichier = 'manquant';
"""
