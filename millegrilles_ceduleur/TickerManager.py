from millegrilles_ceduleur.TickerContext import TickerContext


class TickerManager:

    def __init__(self, context: TickerContext):
        self.__context = context

    @property
    def context(self):
        return self.__context
