import logging
import json
import struct

from io import BufferedReader
from typing import Union


LOGGER = logging.getLogger(__name__)


def lire_header_archive_backup(fp: BufferedReader) -> dict:
    fp.seek(0)

    version_header_info = fp.read(4)
    version, header_len = struct.unpack("HH", version_header_info)
    LOGGER.debug("Version %d, Header length %d", version, header_len)

    header_bytes = fp.read(header_len)
    pos_end = header_bytes.find(0x0)
    header_str = header_bytes[0:pos_end].decode('utf-8')

    LOGGER.info("Header\n*%s*" % header_str)
    return json.loads(header_str)
