from __future__ import annotations

import asyncio
import logging
from xml.etree.ElementTree import ParseError

from redis.exceptions import ResponseError
from sqlalchemy.exc import IntegrityError

from .config import settings
from .db import AsyncSessionLocal
from .redis_client import RedisManager
from .service import ingest_xml
from .tfms_db import TfmsAsyncSessionLocal
from .tfms_service import ingest_tfms_xml
from .tbfm_db import TbfmAsyncSessionLocal
from .tbfm_service import ingest_tbfm_xml

logger = logging.getLogger(__name__)


class StreamWorker:
    def __init__(self, redis_manager: RedisManager) -> None:
        self.redis_manager = redis_manager
        self._task: asyncio.Task | None = None
        self._running = False

    async def ensure_group(self) -> None:
        redis = await self.redis_manager.connect_sfdps()
        try:
            await redis.xgroup_create(settings.raw_stream_name, settings.consumer_group, id='0', mkstream=True)
        except ResponseError as exc:
            if 'BUSYGROUP' not in str(exc):
                raise

    async def start(self) -> None:
        await self.ensure_group()
        self._running = True
        self._task = asyncio.create_task(self.run(), name='faa-stream-worker')

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def run(self) -> None:
        while self._running:
            try:
                redis = await self.redis_manager.connect_sfdps()
                response = await redis.xreadgroup(
                    groupname=settings.consumer_group,
                    consumername=settings.consumer_name,
                    streams={settings.raw_stream_name: '>'},
                    count=settings.worker_count,
                    block=settings.worker_block_ms,
                )
                if not response:
                    continue
                for _, entries in response:
                    for stream_id, fields in entries:
                        xml = fields.get('xml')
                        source = fields.get('source', 'jms-bridge')
                        if not xml:
                            await redis.xack(settings.raw_stream_name, settings.consumer_group, stream_id)
                            continue
                        try:
                            async with AsyncSessionLocal() as session:
                                await ingest_xml(session, self.redis_manager, stream_id=stream_id, source=source, xml_text=xml)
                            await redis.xack(settings.raw_stream_name, settings.consumer_group, stream_id)
                        except IntegrityError as exc:
                            if 'ix_raw_events_stream_id' in str(exc):
                                logger.info('Skipping duplicate SFDPS stream_id=%s', stream_id)
                                await redis.xack(settings.raw_stream_name, settings.consumer_group, stream_id)
                                continue
                            logger.exception('SFDPS integrity error stream_id=%s', stream_id)
                        except Exception:
                            logger.exception('Failed to process stream_id=%s', stream_id)
            except ResponseError as exc:
                if 'NOGROUP' in str(exc):
                    logger.warning('SFDPS consumer group missing; recreating group and stream')
                    await self.ensure_group()
                    await asyncio.sleep(0.2)
                    continue
                logger.exception('SFDPS worker redis response error')
                await asyncio.sleep(1)
            except Exception:
                logger.exception('SFDPS worker loop error')
                await asyncio.sleep(1)


class TfmsStreamWorker:
    def __init__(self, redis_manager: RedisManager) -> None:
        self.redis_manager = redis_manager
        self._task: asyncio.Task | None = None
        self._running = False

    async def ensure_group(self) -> None:
        redis = await self.redis_manager.connect_tfms()
        try:
            await redis.xgroup_create(settings.tfms_raw_stream_name, settings.tfms_consumer_group, id='0', mkstream=True)
        except ResponseError as exc:
            if 'BUSYGROUP' not in str(exc):
                raise

    async def start(self) -> None:
        await self.ensure_group()
        self._running = True
        self._task = asyncio.create_task(self.run(), name='tfms-stream-worker')

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def run(self) -> None:
        while self._running:
            try:
                redis = await self.redis_manager.connect_tfms()
                response = await redis.xreadgroup(
                    groupname=settings.tfms_consumer_group,
                    consumername=settings.tfms_consumer_name,
                    streams={settings.tfms_raw_stream_name: '>'},
                    count=settings.tfms_worker_count,
                    block=settings.tfms_worker_block_ms,
                )
                if not response:
                    continue
                for _, entries in response:
                    for stream_id, fields in entries:
                        xml = fields.get('xml')
                        queue_name = fields.get('queue')
                        if not xml:
                            await redis.xack(settings.tfms_raw_stream_name, settings.tfms_consumer_group, stream_id)
                            continue
                        metadata = {
                            'source': fields.get('source', 'tfms-jms-bridge'),
                            'jms_message_id': fields.get('jms_message_id', ''),
                            'jms_timestamp': fields.get('jms_timestamp', ''),
                            'bridge_received_at': fields.get('bridge_received_at', ''),
                        }
                        try:
                            async with TfmsAsyncSessionLocal() as session:
                                await ingest_tfms_xml(
                                    session,
                                    self.redis_manager,
                                    xml_text=xml,
                                    queue_name=queue_name,
                                    metadata=metadata,
                                )
                            await redis.xack(settings.tfms_raw_stream_name, settings.tfms_consumer_group, stream_id)
                        except ParseError:
                            logger.warning('Ignoring TFMS XML parse error stream_id=%s', stream_id)
                            await redis.xack(settings.tfms_raw_stream_name, settings.tfms_consumer_group, stream_id)
                        except IntegrityError as exc:
                            if 'duplicate key value' in str(exc):
                                logger.info('Skipping duplicate TFMS stream_id=%s', stream_id)
                                await redis.xack(settings.tfms_raw_stream_name, settings.tfms_consumer_group, stream_id)
                                continue
                            logger.exception('TFMS integrity error stream_id=%s', stream_id)
                        except Exception:
                            logger.exception('Failed to process TFMS stream_id=%s', stream_id)
            except ResponseError as exc:
                if 'NOGROUP' in str(exc):
                    logger.warning('TFMS consumer group missing; recreating group and stream')
                    await self.ensure_group()
                    await asyncio.sleep(0.2)
                    continue
                logger.exception('TFMS worker redis response error')
                await asyncio.sleep(1)
            except Exception:
                logger.exception('TFMS worker loop error')
                await asyncio.sleep(1)


class TbfmStreamWorker:
    def __init__(self, redis_manager: RedisManager) -> None:
        self.redis_manager = redis_manager
        self._task: asyncio.Task | None = None
        self._running = False

    async def ensure_group(self) -> None:
        redis = await self.redis_manager.connect_tbfm()
        try:
            await redis.xgroup_create(settings.tbfm_raw_stream_name, settings.tbfm_consumer_group, id='0', mkstream=True)
        except ResponseError as exc:
            if 'BUSYGROUP' not in str(exc):
                raise

    async def start(self) -> None:
        await self.ensure_group()
        self._running = True
        self._task = asyncio.create_task(self.run(), name='tbfm-stream-worker')

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def run(self) -> None:
        while self._running:
            try:
                redis = await self.redis_manager.connect_tbfm()
                response = await redis.xreadgroup(
                    groupname=settings.tbfm_consumer_group,
                    consumername=settings.tbfm_consumer_name,
                    streams={settings.tbfm_raw_stream_name: '>'},
                    count=settings.tbfm_worker_count,
                    block=settings.tbfm_worker_block_ms,
                )
                if not response:
                    continue
                for _, entries in response:
                    for stream_id, fields in entries:
                        xml = fields.get('xml')
                        queue_name = fields.get('queue_name') or fields.get('queue')
                        if not xml:
                            await redis.xack(settings.tbfm_raw_stream_name, settings.tbfm_consumer_group, stream_id)
                            continue
                        metadata = {
                            'source': fields.get('source', 'tbfm-jms-bridge'),
                            'jms_message_id': fields.get('jms_message_id', ''),
                            'jms_timestamp': fields.get('jms_timestamp', ''),
                            'bridge_received_at': fields.get('bridge_received_at', ''),
                        }
                        try:
                            async with TbfmAsyncSessionLocal() as session:
                                await ingest_tbfm_xml(
                                    session,
                                    self.redis_manager,
                                    xml_text=xml,
                                    queue_name=queue_name,
                                    metadata=metadata,
                                )
                            await redis.xack(settings.tbfm_raw_stream_name, settings.tbfm_consumer_group, stream_id)
                        except (ParseError, ValueError):
                            logger.warning('Ignoring TBFM XML parse error stream_id=%s', stream_id)
                            await redis.xack(settings.tbfm_raw_stream_name, settings.tbfm_consumer_group, stream_id)
                        except IntegrityError as exc:
                            if 'duplicate key value' in str(exc):
                                logger.info('Skipping duplicate TBFM stream_id=%s', stream_id)
                                await redis.xack(settings.tbfm_raw_stream_name, settings.tbfm_consumer_group, stream_id)
                                continue
                            logger.exception('TBFM integrity error stream_id=%s', stream_id)
                        except Exception:
                            logger.exception('Failed to process TBFM stream_id=%s', stream_id)
            except ResponseError as exc:
                if 'NOGROUP' in str(exc):
                    logger.warning('TBFM consumer group missing; recreating group and stream')
                    await self.ensure_group()
                    await asyncio.sleep(0.2)
                    continue
                logger.exception('TBFM worker redis response error')
                await asyncio.sleep(1)
            except Exception:
                logger.exception('TBFM worker loop error')
                await asyncio.sleep(1)
