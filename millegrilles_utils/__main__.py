import argparse
import asyncio
import logging

from millegrilles_utils.Backup import main as backup_main


def parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Demarrer une application midcompte/certissuer/utils de MilleGrilles")
    parser.add_argument(
        '--verbose', action="store_true", required=False,
        help="Active le logging maximal"
    )
    # parser.add_argument('command', choices=['midcompte', 'certissuer', 'backup'], help='Commande')

    subparsers = parser.add_subparsers(dest='command', required=True, help="Commandes")

    subparser_midcompte = subparsers.add_parser('midcompte', help='middleware compte handler')

    subparser_certissuer = subparsers.add_parser('certissuer', help='cert issuer')

    # Subparser backup
    subparser_backup = subparsers.add_parser('backup', help='Backup de fichiers')
    subparser_backup.add_argument('--source', default='/var/opt/millegrilles_backup',
                                  help='Repertoire source du backup')
    subparser_backup.add_argument('--dest', default='/var/opt/millegrilles_backup/_ARCHIVES',
                                  help='Repertoire destination du backup')
    subparser_backup.add_argument('--ca', default='/var/opt/millegrilles/configuration/pki.millegrille.cert',
                                  help='Certificat de MilleGrille')

    args = parser.parse_args()
    adjust_logging(args)

    return args


def adjust_logging(args: argparse.Namespace):
    if args.verbose is True:
        loggers = [__name__, 'millegrilles_messages', 'millegrilles_certissuer', 'millegrilles_midcompte', 'millegrilles_utils']
        for log in loggers:
            logging.getLogger(log).setLevel(logging.DEBUG)


async def demarrer(args: argparse.Namespace):
    command = args.command

    if command == 'midcompte':
        raise NotImplementedError('todo')
    elif command == 'certissuer':
        raise NotImplementedError('todo')
    elif command == 'backup':
        await backup_main(args.source, args.dest, args.ca)
    else:
        raise ValueError('non supporte')


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
