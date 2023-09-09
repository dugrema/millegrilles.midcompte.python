CONST_CREATE_FICHIERS = """
    CREATE TABLE IF NOT EXISTS FICHIERS(
        fuuid VARCHAR PRIMARY KEY,
        taille INTEGER,
        etat_fichier VARCHAR NOT NULL,
        date_presence TIMESTAMP NOT NULL,
        date_verification TIMESTAMP NOT NULL,
        date_reclamation TIMESTAMP NOT NULL     
    );
"""

CONST_INSERT_FICHIER = """
    INSERT INTO FICHIERS(fuuid, taille, etat_fichier, date_presence, date_verification, date_reclamation)
    VALUES(:fuuid, :taille, :etat_fichier, :date_presence, :date_verification, :date_reclamation);
"""

CONST_ACTIVER_SI_MANQUANT = """
    UPDATE FICHIERS
    SET etat_fichier = :etat_fichier
    WHERE fuuid = :fuuid AND etat_fichier = 'manquant'
"""

CONST_VERIFIER_FICHIER = """
    UPDATE FICHIERS
    SET taille = :taille, 
        date_presence = datetime('now', 'utc'), 
        date_verification = :date_verification
    WHERE fuuid = :fuuid
"""

CONST_RECLAMER_FICHIER = """
    UPDATE FICHIERS
    SET date_reclamation = :date_reclamation
    WHERE fuuid = :fuuid
"""

CONST_STATS_FICHIERS = """
    SELECT etat_fichier, count(fuuid) as nombre, sum(taille) as taille
    FROM fichiers
    GROUP BY etat_fichier;
"""

CONST_INFO_FICHIER = """
    SELECT fuuid, taille, etat_fichier, date_presence, date_verification, date_reclamation
    FROM fichiers 
    WHERE fuuid = :fuuid;
"""
