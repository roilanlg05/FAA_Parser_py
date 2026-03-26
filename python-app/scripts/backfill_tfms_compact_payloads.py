from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from sqlalchemy import select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.tfms_db import TfmsAsyncSessionLocal
from app.tfms_models import TfmsEvent, TfmsProjection
from app.tfms_payload_utils import strip_raw_fields


async def backfill_events(batch_size: int, max_batches: int | None) -> tuple[int, int]:
    scanned = 0
    updated = 0
    last_id = 0
    batches = 0
    while True:
        if max_batches is not None and batches >= max_batches:
            return scanned, updated
        async with TfmsAsyncSessionLocal() as session:
            result = await session.execute(
                select(TfmsEvent).where(TfmsEvent.id > last_id).order_by(TfmsEvent.id.asc()).limit(batch_size)
            )
            rows = list(result.scalars())
            if not rows:
                return scanned, updated
            for row in rows:
                scanned += 1
                compact = strip_raw_fields(row.parsed_json or {})
                if compact != (row.parsed_json or {}):
                    row.parsed_json = compact
                    updated += 1
                last_id = row.id
            await session.commit()
            batches += 1


async def backfill_projections(batch_size: int, max_batches: int | None) -> tuple[int, int]:
    scanned = 0
    updated = 0
    last_key = ''
    batches = 0
    while True:
        if max_batches is not None and batches >= max_batches:
            return scanned, updated
        async with TfmsAsyncSessionLocal() as session:
            result = await session.execute(
                select(TfmsProjection)
                .where(TfmsProjection.projection_key > last_key)
                .order_by(TfmsProjection.projection_key.asc())
                .limit(batch_size)
            )
            rows = list(result.scalars())
            if not rows:
                return scanned, updated
            for row in rows:
                scanned += 1
                compact = strip_raw_fields(row.data or {})
                if compact != (row.data or {}):
                    row.data = compact
                    updated += 1
                last_key = row.projection_key
            await session.commit()
            batches += 1


async def main(batch_size: int, max_batches: int | None) -> None:
    events_scanned, events_updated = await backfill_events(batch_size, max_batches)
    projections_scanned, projections_updated = await backfill_projections(batch_size, max_batches)
    print(
        f'events_scanned={events_scanned} events_updated={events_updated} '
        f'projections_scanned={projections_scanned} projections_updated={projections_updated}'
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Backfill TFMS compact JSON payloads (strip embedded raw branches)')
    parser.add_argument('--batch-size', type=int, default=500, help='rows per transaction batch')
    parser.add_argument('--max-batches', type=int, default=None, help='optional cap per table for incremental runs')
    args = parser.parse_args()
    asyncio.run(main(args.batch_size, args.max_batches))
