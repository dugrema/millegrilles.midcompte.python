from typing import Optional, TypedDict


class SecretKeyDict(TypedDict):
    cle_id: str
    cle_secrete_base64: str
    format: Optional[str]
    header: Optional[str]
    nonce: Optional[str]
    secret: Optional[bytes]

class VersionItem(TypedDict):
    fuuid: str
    mimetype: str
    taille: int
    tuuids: list[str]
    visites: dict[str, int]
    cle_id: Optional[str]
    format: Optional[str]
    nonce: Optional[str]

class MediaItem(TypedDict):
    pass

class VersionJob(TypedDict):
    version: VersionItem
    media: Optional[MediaItem]
    keys: Optional[dict[str, SecretKeyDict]]
    decrypted_key: Optional[bytes]
