import asyncio
import logging
import json

#from threading import Event
from asyncio import Event
from asyncio.exceptions import TimeoutError
from pika.exchange_type import ExchangeType

from millegrilles_messages.messages import Constantes
from millegrilles_messages.messages.MessagesThread import MessagesThread
from millegrilles_messages.messages.MessagesModule import RessourcesConsommation, ExchangeConfiguration

logger = logging.getLogger(__name__)

LOGGING_FORMAT = '%(asctime)s %(threadName)s %(levelname)s: %(message)s'


async def main():
    logger.info("Debut main()")
    stop_event = Event()

    # Preparer resources consumer
    reply_res = RessourcesConsommation(callback_reply_q)

    messages_thread = MessagesThread(stop_event)
    messages_thread.set_reply_ressources(reply_res)

    # Demarrer traitement messages
    await messages_thread.start_async()
    fut_run = messages_thread.run_async()
    fut_run_tests = run_tests(messages_thread, stop_event)

    tasks = [
        asyncio.create_task(fut_run),
        asyncio.create_task(fut_run_tests),
    ]

    # Execution de la loop avec toutes les tasks
    await asyncio.tasks.wait(tasks, return_when=asyncio.tasks.FIRST_COMPLETED)

    logger.info("Fin main()")


async def run_tests(messages_thread, stop_event):
    producer = messages_thread.get_producer()

    # Demarrer test (attendre connexion prete)
    logger.info("Attendre pret")
    await messages_thread.attendre_pret()
    logger.info("produire messages")

    reply_q = await producer.get_reply_q()
    # cert = await producer.executer_commande(
    #     dict(), domaine='MaitreDesCles', action='certMaitreDesCles',
    #     exchange=Constantes.SECURITE_PRIVE,
    #     reply_to=reply_q,
    #     nowait=False)
    cert = await producer.executer_requete(
        dict(), domaine='MaitreDesCles', action='certMaitreDesCles', exchange=Constantes.SECURITE_PRIVE)

    logger.info("Certificat : %s" % cert)
    try:
        await asyncio.wait_for(stop_event.wait(), 300)
    except TimeoutError:
        pass
    stop_event.set()


async def callback_reply_q(message, module_messages: MessagesThread):
    message_parsed = message.parsed

    try:
        cles = message_parsed['cles']
        logger.info("Cles recues : %s" % json.dumps(cles, indent=2))

        # Faire une requete pour l'archive a dechiffrer
        producer = module_messages.get_producer()

        for nom_fichier, cle in cles.items():
            requete = {'fichierBackup': nom_fichier}

            # Note : bounce vers Q1 pour permettre de faire la requete sur le fichier (besoin thread reply_q)
            await producer.executer_commande(requete, domaine='instance', action='test_backup', exchange='2.prive', nowait=True)

    except KeyError:
        logger.info("Message recu : %s" % json.dumps(message_parsed, indent=2))

    # wait_event.wait(0.7)


async def callback_q_1(message, module_messages: MessagesThread):
    logger.info("callback_q_1 Message recu : %s" % json.dumps(message.parsed, indent=2))
    producer = module_messages.get_producer()

    # Bounce la requete de fichier de backup
    requete = {'fichierBackup': message.parsed['fichierBackup']}
    resultat = await producer.executer_requete(requete, domaine='fichiers', action='getBackupTransaction', exchange='2.prive')
    transaction_backup = resultat.parsed['backup']
    logger.info("Fichier transaction : %s" % json.dumps(transaction_backup, indent=2))


def callback_q_2(message, module_messages):
    logger.info("callback_q_2 Message recu : %s" % message)


if __name__ == '__main__':
    # logging.basicConfig()
    logging.basicConfig(format=LOGGING_FORMAT, level=logging.WARN)
    logging.getLogger(__name__).setLevel(logging.DEBUG)
    logging.getLogger('millegrilles').setLevel(logging.DEBUG)
    asyncio.run(main())
