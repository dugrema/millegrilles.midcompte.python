from millegrilles_messages.messages import Constantes as ConstantesMilleGrilles
from millegrilles_messages.messages.MessagesModule import MessageWrapper
from millegrilles_messages.MilleGrillesConnecteur import EtatInstance


class HandlerRestauration:

    def __init__(self, etat_instance: EtatInstance):
        self.__etat_instance = etat_instance

    async def restaurer(self, message: MessageWrapper):

        # Arreter le declenchement des triggers de backup
        self.__etat_instance.backup_inhibe = True

        try:
            raise NotImplementedError('todo')
        finally:
            # Reactiver les triggers de backup
            self.__etat_instance.backup_inhibe = False
