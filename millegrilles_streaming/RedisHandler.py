import asyncio
import datetime
import json
import logging
import pytz
import redis.asyncio as redis

from typing import Optional

from redis.asyncio.client import Redis as RedisClient

from millegrilles_streaming.Constantes import REDIS_SESSION_DATABASE
from millegrilles_streaming.Context import StreamingContext

LOGGER = logging.getLogger(__name__)


class RedisClientDao:

    def __init__(self, context: StreamingContext):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__context = context
        self.__redis_client: Optional[RedisClient] = None

    async def connect(self):
        self.__redis_client = await _connect_redis(self.__context)

    async def set_token(self, token: str, expiration: int, fuuid: str, mimetype: str):
        if not self.__redis_client:
            raise Exception("Redis client not set")
        now_epoch = datetime.datetime.now(tz=pytz.UTC)
        duration = int(expiration - now_epoch.timestamp())
        self.__logger.info(f'Adding token ...{token[-4:]} with duration {duration}')
        if duration < 0:
            raise Exception("Token expired")
        content = json.dumps({'fuuid': fuuid, 'mimetype': mimetype})
        await self.__redis_client.setex(f"stream.{token}", duration, content)

    async def check_token(self, token: str) -> Optional[dict]:
        if not self.__redis_client:
            raise Exception("Redis client not set")
        content_str: Optional[str] = await self.__redis_client.get(f"stream.{token}")
        if not content_str:
            return None
        return json.loads(content_str)


async def _connect_redis(context: StreamingContext) -> RedisClient:
    configuration_app = context.configuration
    redis_hostname = configuration_app.redis_hostname
    redis_port = configuration_app.redis_port
    redis_username = configuration_app.redis_username
    with open(configuration_app.redis_password_path, 'rt') as fp:
        redis_password = await asyncio.to_thread(fp.readline, 1024)

    key_path = configuration_app.key_path
    cert_path = configuration_app.cert_path or configuration_app.key_path
    ca_path = configuration_app.ca_path
    redis_database_val = REDIS_SESSION_DATABASE

    url_redis = f"rediss://{redis_hostname}:{redis_port}"

    LOGGER.info("Connecting to redis for web sessions : %s", url_redis)

    redis_client = await redis.from_url(
        url_redis, db=redis_database_val, username=redis_username, password=redis_password,
        ssl_keyfile=key_path, ssl_certfile=cert_path, ssl_ca_certs=ca_path,
        ssl_cert_reqs="required", ssl_check_hostname=True,
    )

    return redis_client
