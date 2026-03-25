from __future__ import annotations

import json
from typing import Any

from redis.asyncio import Redis

from .config import settings


class RedisManager:
    def __init__(self) -> None:
        self.sfdps_client: Redis | None = None
        self.tfms_client: Redis | None = None
        self.tbfm_client: Redis | None = None

    async def connect_sfdps(self) -> Redis:
        if self.sfdps_client is None:
            self.sfdps_client = Redis.from_url(settings.sfdps_redis_url, decode_responses=True)
        return self.sfdps_client

    async def connect_tfms(self) -> Redis:
        if self.tfms_client is None:
            self.tfms_client = Redis.from_url(settings.tfms_redis_url, decode_responses=True)
        return self.tfms_client

    async def connect_tbfm(self) -> Redis:
        if self.tbfm_client is None:
            self.tbfm_client = Redis.from_url(settings.tbfm_redis_url, decode_responses=True)
        return self.tbfm_client

    async def close(self) -> None:
        if self.sfdps_client is not None:
            await self.sfdps_client.close()
            self.sfdps_client = None
        if self.tfms_client is not None:
            await self.tfms_client.close()
            self.tfms_client = None
        if self.tbfm_client is not None:
            await self.tbfm_client.close()
            self.tbfm_client = None

    async def publish_parsed(self, payload: dict[str, Any]) -> None:
        client = await self.connect_sfdps()
        data = json.dumps(payload, ensure_ascii=False)
        await client.publish(settings.parsed_channel_name, data)
        await client.xadd(settings.parsed_stream_name, {'json': data}, maxlen=100_000, approximate=True)

    async def enqueue_raw_xml(self, xml: str, source: str) -> str:
        client = await self.connect_sfdps()
        return await client.xadd(settings.raw_stream_name, {'xml': xml, 'source': source}, maxlen=100_000, approximate=True)
