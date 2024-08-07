from typing import Optional

from millegrilles_backup import Constantes
from millegrilles_messages.MilleGrillesConnecteur import Configuration as ConfigurationAbstract

CONST_BACKUP_PARAMS = [
    Constantes.ENV_DIR_BACKUP,
]


class ConfigurationBackup(ConfigurationAbstract):

    def __init__(self):
        super().__init__()
        self.dir_backup = '/var/opt/millegrilles/consignation/backupService'

    def get_params_list(self) -> list:
        params = super().get_params_list()
        params.extend(CONST_BACKUP_PARAMS)
        return params

    def parse_config(self, configuration: Optional[dict] = None):
        """
        Conserver l'information de configuration
        :param configuration:
        :return:
        """
        dict_params = super().parse_config(configuration)

        # Params optionnels
        self.dir_backup = dict_params.get(Constantes.ENV_DIR_BACKUP) or self.dir_backup
