from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    app_name: str = os.getenv('APP_NAME', 'faa-swim-stack')
    database_url: str = os.getenv('DATABASE_URL', 'postgresql+asyncpg://postgres:postgres@localhost:5432/faa_swim')
    tfms_database_url: str = os.getenv('TFMS_DATABASE_URL', 'postgresql+asyncpg://postgres:postgres@localhost:5432/tfms')
    sfdps_redis_url: str = os.getenv('SFDPS_REDIS_URL', os.getenv('REDIS_URL', 'redis://localhost:6379/0'))
    tfms_redis_url: str = os.getenv('TFMS_REDIS_URL', os.getenv('REDIS_URL', 'redis://localhost:6379/0'))
    tbfm_redis_url: str = os.getenv('TBFM_REDIS_URL', os.getenv('REDIS_URL', 'redis://localhost:6379/0'))
    raw_stream_name: str = os.getenv('RAW_STREAM_NAME', 'faa.raw.xml')
    parsed_stream_name: str = os.getenv('PARSED_STREAM_NAME', 'faa.parsed.json')
    parsed_channel_name: str = os.getenv('PARSED_CHANNEL_NAME', 'faa.parsed.events')
    consumer_group: str = os.getenv('CONSUMER_GROUP', 'faa-python-parser')
    consumer_name: str = os.getenv('CONSUMER_NAME', 'worker-1')
    worker_block_ms: int = int(os.getenv('WORKER_BLOCK_MS', '5000'))
    worker_count: int = int(os.getenv('WORKER_COUNT', '20'))
    enable_worker: bool = os.getenv('ENABLE_WORKER', 'true').lower() == 'true'
    tfms_events_channel_name: str = os.getenv('TFMS_EVENTS_CHANNEL_NAME', 'tfms.events')
    tfms_projections_channel_name: str = os.getenv('TFMS_PROJECTIONS_CHANNEL_NAME', 'tfms.projections')
    tfms_raw_stream_name: str = os.getenv('TFMS_RAW_STREAM_NAME', 'tfms.raw.xml')
    tfms_consumer_group: str = os.getenv('TFMS_CONSUMER_GROUP', 'tfms-python-parser')
    tfms_consumer_name: str = os.getenv('TFMS_CONSUMER_NAME', 'tfms-worker-1')
    tfms_worker_block_ms: int = int(os.getenv('TFMS_WORKER_BLOCK_MS', '5000'))
    tfms_worker_count: int = int(os.getenv('TFMS_WORKER_COUNT', '20'))
    tfms_queue_name: str = os.getenv(
        'TFMS_QUEUE_NAME',
        'landgbnb.gmail.com.TFMS.3b02c856-4246-4fa3-8c92-a7c2a37d2176.OUT',
    )
    tfms_parser_path: str = os.getenv(
        'TFMS_PARSER_PATH',
        '/Users/hashdown/Projects/FAA_Parser_py/python-app/tfms/app/tfms_parser.py',
    )
    tfms_raw_response_from_xml: bool = os.getenv('TFMS_RAW_RESPONSE_FROM_XML', 'true').lower() in {
        '1',
        'true',
        'yes',
        'on',
    }
    tbfm_database_url: str = os.getenv('TBFM_DATABASE_URL', 'postgresql+asyncpg://postgres:postgres@localhost:5432/tbfm')
    tbfm_events_channel_name: str = os.getenv('TBFM_EVENTS_CHANNEL_NAME', 'tbfm.events')
    tbfm_projections_channel_name: str = os.getenv('TBFM_PROJECTIONS_CHANNEL_NAME', 'tbfm.projections')
    tbfm_raw_stream_name: str = os.getenv('TBFM_RAW_STREAM_NAME', 'tbfm.raw.xml')
    tbfm_consumer_group: str = os.getenv('TBFM_CONSUMER_GROUP', 'tbfm-python-parser')
    tbfm_consumer_name: str = os.getenv('TBFM_CONSUMER_NAME', 'tbfm-worker-1')
    tbfm_worker_block_ms: int = int(os.getenv('TBFM_WORKER_BLOCK_MS', '5000'))
    tbfm_worker_count: int = int(os.getenv('TBFM_WORKER_COUNT', '20'))
    tbfm_queue_name: str = os.getenv(
        'TBFM_QUEUE_NAME',
        'landgbnb.gmail.com.TBFM.3766a42b-c0b9-4848-9898-2e7bff589b66.OUT',
    )
    tbfm_parser_path: str = os.getenv(
        'TBFM_PARSER_PATH',
        '/Users/hashdown/Projects/FAA_Parser_py/python-app/tbfm/app/tbfm_parser.py',
    )
    tbfm_projections_path: str = os.getenv(
        'TBFM_PROJECTIONS_PATH',
        '/Users/hashdown/Projects/FAA_Parser_py/python-app/tbfm/app/projections.py',
    )
    tbfm_raw_response_from_xml: bool = os.getenv('TBFM_RAW_RESPONSE_FROM_XML', 'true').lower() in {
        '1',
        'true',
        'yes',
        'on',
    }


settings = Settings()
