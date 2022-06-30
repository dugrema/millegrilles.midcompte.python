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
    exchange_1 = ExchangeConfiguration("1.public", ExchangeType.topic)

    reply_res = RessourcesConsommation(callback_reply_q)
    # q1 = RessourcesConsommation(callback_q_1, 'CoreBackup/tada')
    # q1.ajouter_rk('3.protege', 'commande.CoreBackup.m1')
    # q1.ajouter_rk('2.prive', 'commande.CoreBackup.m2')
    #
    # q2 = RessourcesConsommation(callback_q_2, 'CoreBackup/titi', durable=True)
    # q2.set_ttl(30000)
    # q2.ajouter_rk('2.prive', 'evenement.CoreBackup.t1')
    # q2.ajouter_rk('2.prive', 'commande.CoreBackup.t2')

    messages_thread = MessagesThread(stop_event)
    messages_thread.set_reply_ressources(reply_res)
    # messages_thread.ajouter_consumer(q1)
    # messages_thread.ajouter_consumer(q2)

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

    reply_q = producer.get_reply_q()
    await producer.executer_commande(
        dict(), domaine='fichiers', action='getClesBackupTransactions',
        exchange=Constantes.SECURITE_PRIVE, nowait=True)

    logger.info("Attente")
    try:
        await asyncio.wait_for(stop_event.wait(), 300)
    except TimeoutError:
        pass
    stop_event.set()


async def callback_reply_q(message, module_messages):
    message_parsed = message.parsed
    logger.info("Message recu : %s" % json.dumps(message_parsed, indent=2))

    # wait_event.wait(0.7)


def callback_q_1(message, module_messages):
    logger.info("callback_q_1 Message recu : %s" % message)


def callback_q_2(message, module_messages):
    logger.info("callback_q_2 Message recu : %s" % message)


if __name__ == '__main__':
    # logging.basicConfig()
    logging.basicConfig(format=LOGGING_FORMAT, level=logging.WARN)
    logging.getLogger(__name__).setLevel(logging.DEBUG)
    logging.getLogger('millegrilles').setLevel(logging.DEBUG)
    asyncio.run(main())
