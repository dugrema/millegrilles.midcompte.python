# Utilitaires middleware midcompte et al.

## backup

Application de backup de transactions. Cette application ne gère pas les fichiers.

Chaque application de domaine MilleGrilles répond au messages de l'utilitaire de backup pour émettre une copie de
ses transactions via MQ. L'utilitaire de backup reçoit, vérifie et conserve dans un répertoire local ces transactions.

Si une application de fichiers (primaire ou secondaire) est configurée sur l'instance locale, les backups sont uploadés 
régulièrement pour en conserver une copie. Ces copies sont par la suite synchronisées sur tous les miroirs. 

Déploiement : optionnel, maximum 1 sur la MilleGrille

Sécurité : 2.prive

Connectivité:
* mq
* fichiers (optionnel) sur /fichiers_transfert/backup

## ceduleur

Le céduleur est une application qui émet un message _trigger_ à toutes les minutes (environ 2 secondes après la minute).
Ce trigger est utilisé par tous les services pour exécuter des tâches. Le céduleur doit être déployé exactement 1 fois
sur une millegrille.

Déploiement : requis, exactement 1 sur la MilleGrille

Sécurité : 3.protege

Connectivité :
* mq

## certissuer

Le certissuer possède un certificat intermédiaire (signé par la clé de millegrille) qui lui permet de générer n'importe
quel autre type de certificat. Une millegrille doit avoir au moins 1 certissuer déployé sur une instance 3.protege 
ou 4.secure.

Déploiement : requis, au moins 1 sur la MilleGrille

Sécurité : 3.protege

Connectivité : 
* port server 2080 (http)

## fichiers

L'application fichiers agit comme système de stockage pour le contenu binaire géré par les applications. Une millegrille
devrait avoir une instance principale de fichiers et en option plusieurs instances secondaires qui agissent comme miroirs.

Déploiement : optionnel, aucunes limites

Sécurité : 2.prive

Connectivité : 
* port server TLS (par défaut 444) sur path /fichiers_transfert
* reseau docker port server 3021
* mq
* connexion à d'autres instance de fichiers pour synchronisation (miroir) via lien principal <- secondaire

Note : l'applications fichiers est un élément central pour le transfert de contenu d'une MilleGrille. L'application
       CoreTopologie gère le rôle des instances fichiers (primaire ou secondaire) et s'occupe de donner un URL de 
       connexion https aux autres applications qui veulent utiliser les ressources de l'application fichiers. 
       Ces éléments peuvent être configurés sous Coupdoeil, Menu Domaines / Fichiers. Les instances de fichiers sont
       détectées automatiquement sur les instances de MilleGrilles connectées à MQ.

## media

L'application media effectue le traitement de fichiers media. Elle génére des thumbnails pour les images et certains
types de documents (e.g. PDF). Elle converti aussi les vidéos en formats utilisables par appareils mobiles.

Note de sécurité : Le module média a la permission de déchiffrer n'importe quel contenu avec le mimetype 
images ou video, et certains autres types (e.g. application/pdf). Ceci est un risque de sécurité. Si possible, déployer
ce module dans un environnement à sécurité OS et physique accrus.

Sécurité : 3.protege

Connectivité :
* mq
* fichiers sur /fichiers_transfert

## midcompte

## relaiweb

## solr

## streaming
