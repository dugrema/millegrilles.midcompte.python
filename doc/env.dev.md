# Utilitaires middleware

## backup

## ceduleur

Ajouter ces parametres d'environnement
<pre>
REDIS_PASSWORD_PATH=/var/opt/millegrilles/secrets/passwd.redis.txt
</pre>

Fermer l'instance ceduleur dans docker :

<pre>
docker service scale ceduleur=0
</pre>

## certissuer

Module : millegrilles_certissuer

Récupérer l'instance ID avec la commande suivante :

`cat /var/opt/millegrilles/configuration/instance_id.txt`

Ajouter ces parametres d'environnement
<pre>
INSTANCE_ID=_VOTRE instance_id_
REDIS_PASSWORD_PATH=/var/opt/millegrilles/secrets/passwd.redis.txt
</pre>

Fermer l'instance certissuer dans docker :

<pre>
docker service scale certissuer=0
</pre>

Démarrer certissuer.

Note : le paramètre --verbose permetre d'augmenter le niveau de logging pour le développement.

## fichiers

Paramètres d'environnement

<pre>
MQ_HOSTNAME=localhost
REDIS_HOSTNAME=localhost
CA_PEM=/var/opt/millegrilles/configuration/pki.millegrille.cert
CERT_PEM=/var/opt/millegrilles/secrets/pki.fichiers.cert
KEY_PEM=/var/opt/millegrilles/secrets/pki.fichiers.cle
REDIS_PASSWORD_PATH=/var/opt/millegrilles/secrets/passwd.redis.txt
SFTP_ED25519_KEY=/var/opt/millegrilles/secrets/passwd.fichiers_ed25519.txt
SFTP_RSA_KEY=/var/opt/millegrilles/secrets/passwd.fichiers_rsa.txt
WEB_PORT=3021
</pre>

Paramètres optionnels avec exemple:
DIR_CONSIGNATION=/var/opt/millegrilles/consignation2
DIR_DATA=/var/opt/millegrilles/consignation2/data

Fermer l'instance fichiers dans docker :

<pre>
docker service scale fichiers=0
</pre>

Aller dans Coupdoeil, menu Domaines / Fichiers.
Cliquer sur l'instance de fichiers et modifier le paramètre de connexion pour : 
https://IP_LOCALE:3021

