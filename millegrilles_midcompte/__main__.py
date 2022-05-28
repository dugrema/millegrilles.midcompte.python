import asyncio

from millegrilles_midcompte.Midcompte import demarrer


def main():
    """
    Methode d'execution de l'application
    :return:
    """
    asyncio.run(demarrer())


if __name__ == '__main__':
    main()
