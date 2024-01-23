# Utilitaires middleware

Pour travailler sur une application il faut l'installer avec CoupDoeil. Ceci permet d'obtenir un certificat pour
la connexion a MQ et aux autres modules.

## backup

Ajouter ces parametres d'environnement
<pre>
CERT_PEM=/var/opt/millegrilles/secrets/pki.backup.cert
KEY_PEM=/var/opt/millegrilles/secrets/pki.backup.cle
MQ_HOSTNAME=localhost
REDIS_HOSTNAME=localhost
REDIS_PASSWORD_PATH=/var/opt/millegrilles/secrets/passwd.redis.txt
</pre>

Fermer l'instance backup dans docker :

<pre>
docker service scale backup=0
</pre>

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

## media

Aucune configuration d'environnement particulière n'est requise.

Fermer l'instance midcompte dans docker :

<pre>
docker service scale media=0
</pre>


## midcompte

Ajouter ces parametres d'environnement
<pre>
CA_PEM=/var/opt/millegrilles/configuration/pki.millegrille.cert
CERT_PEM=/var/opt/millegrilles/secrets/pki.midcompte.cert
KEY_PEM=/var/opt/millegrilles/secrets/pki.midcompte.cle
MONGO_HOSTNAME=localhost
MONGO_PASSWORD_PATH=/var/opt/millegrilles/secrets/passwd.mongo.txt
MQ_PASSWORD_PATH=/var/opt/millegrilles/secrets/passwd.mqadmin.txt
MQ_URL=https://localhost:8443
</pre>

Fermer l'instance midcompte dans docker :

<pre>
docker service scale midcompte=0
</pre>

Note : changer mapping sous configuration nginx (port 2444)

## relaiweb

Ajouter ces parametres d'environnement

<pre>
CERT_PEM=/var/opt/millegrilles/secrets/pki.relaiweb.cert
KEY_PEM=/var/opt/millegrilles/secrets/pki.relaiweb.cle
MQ_HOSTNAME=localhost
REDIS_HOSTNAME=localhost
REDIS_PASSWORD_PATH=/var/opt/millegrilles/secrets/passwd.redis.txt
</pre>

Fermer l'instance midcompte dans docker :

<pre>
docker service scale relaiweb=0
</pre>

## solr

docker service update --publish-add 8983:8983 solr_server

<pre>
SOLR_URL=https://`HOSTNAME`:8983
</pre>

## streaming

Ajouter ces parametres d'environnement

<pre>
CA_PEM=/var/opt/millegrilles/configuration/pki.millegrille.cert
CERT_PEM=/var/opt/millegrilles/secrets/pki.stream.cert
DIR_STAGING=/var/opt/millegrilles/consignation/staging/streaming
KEY_PEM=/var/opt/millegrilles/secrets/pki.stream.cle
MQ_HOSTNAME=localhost
REDIS_HOSTNAME=localhost
REDIS_PASSWORD_PATH=/var/opt/millegrilles/secrets/passwd.redis.txt
WEB_PORT=3029
</pre>

Modifier fichier /var/opt/millegrilles/nginx/modules/stream.app.proxypass :
Changer ligne proxy_pass, utiliser celle avec `SERVER` et remplacer pour l'adresse locale (IP locale si en mode offline)

Executer changements docker :

<pre>
docker service scale stream=0
docker service update --force nginx
</pre>
