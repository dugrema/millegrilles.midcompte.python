import pathlib

from typing import Optional


class InformationFuuid:

    def __init__(self, fuuid: str, jwt_token: Optional[str] = None, params: Optional[dict] = None):
        self.fuuid = fuuid
        self.jwt_token = jwt_token
        self.user_id: Optional[str] = None                  # User_id, requis pour recuperer les cles
        self.taille: Optional[int] = None                   # Taille du fichier
        self.mimetype: Optional[str] = None                 # Mimetype du fichier dechiffre
        # self.status: Optional[int] = None                 # Status du back-end/consignation
        self.position_courante: Optional[int] = None        # Position courante de dechiffrage
        self.path_complet: Optional[pathlib.Path] = None    # Path complet sur disque du fichier dechiffre

        self.ref: Optional[str] = None                  # Fuuid de reference (pour cle)
        self.header: Optional[str] = None               # Header de dechiffrage
        self.nonce: Optional[str] = None                # Nonce de dechiffrage
        self.format: Optional[str] = None               # Format de dechiffrage

        # self.event_pret: Optional[asyncio.Event] = None  # Set quand le download est pret

        if params is not None:
            self.set_params(params)

    # async def init(self):
    #     self.event_pret = asyncio.Event()

    def set_params(self, params: dict):
        self.taille = params.get('taille')
        self.mimetype = params.get('mimetype')
        # self.status = params.get('status')
        self.user_id = params.get('userId') or params.get('user_id')
        self.ref = params.get('ref')
        self.header = params.get('header')
        self.nonce = params.get('nonce')
        self.format = params.get('format')


    # @property
    # def est_pret(self):
    #     if self.event_pret is not None:
    #         return self.event_pret.is_set()
    #     return self.path_complet is not None

    def get_params_dechiffrage(self):
        if self.nonce is not None:
            nonce = self.nonce
        else:
            nonce = self.header[1:]  # Retirer 'm' multibase
        return {
            'nonce': nonce,
            'format': self.format,
        }


class FilehostInvalidException(Exception):
    pass
