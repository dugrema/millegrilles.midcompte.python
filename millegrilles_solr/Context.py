import logging

from millegrilles_solr.Configuration import ConfigurationRelaiSolr
from millegrilles_messages.bus.BusContext import MilleGrillesBusContext

LOGGER = logging.getLogger(__name__)


class SolrContext(MilleGrillesBusContext):

    def __init__(self, configuration: ConfigurationRelaiSolr):
        super().__init__(configuration)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
