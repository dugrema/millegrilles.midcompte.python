import argparse
import asyncio
import logging
import signal

from millegrilles_relaiweb.RelaiWeb import RelaiWeb


def parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Demarrer une application midcompte/certissuer/utils de MilleGrilles")
    parser.add_argument(
        '--verbose', action="store_true", required=False,
        help="Active le logging maximal"
    )

    args = parser.parse_args()
    adjust_logging(args)

    return args


def adjust_logging(args: argparse.Namespace):
    if args.verbose is True:
        loggers = [__name__, 'millegrilles_messages']
        for log in loggers:
            logging.getLogger(log).setLevel(logging.DEBUG)


async def demarrer(args: argparse.Namespace):
    config = dict()

    relai_web = RelaiWeb()
    await relai_web.charger_configuration(args)

    signal.signal(signal.SIGINT, relai_web.exit_gracefully)
    signal.signal(signal.SIGTERM, relai_web.exit_gracefully)

    # generateur.preparer_chiffrage()
    await relai_web.run()


def main():
    """
    Methode d'execution de l'application
    :return:
    """
    logging.basicConfig()
    logging.getLogger(__name__).setLevel(logging.INFO)

    args = parse()
    asyncio.run(demarrer(args))


if __name__ == '__main__':
    main()
