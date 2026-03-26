from __future__ import annotations

import argparse
import asyncio
import sys
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import AsyncSessionLocal
from app.db import engine as sfdps_engine


@dataclass
class MaintenanceResult:
    archived: int
    deleted: int


async def baseline() -> None:
    async with AsyncSessionLocal() as session:
        tables = await session.execute(
            text(
                """
                SELECT relname, pg_total_relation_size(relid) AS bytes
                FROM pg_catalog.pg_statio_user_tables
                ORDER BY pg_total_relation_size(relid) DESC
                """
            )
        )
        print('table_sizes_bytes')
        for name, size_bytes in tables.all():
            print(f'{name}|{size_bytes}')

        counts = await session.execute(
            text(
                """
                SELECT
                  (SELECT COUNT(*) FROM raw_events) AS raw_events_total,
                  (SELECT COUNT(*) FROM raw_events_archive) AS raw_events_archive_total,
                  (SELECT COUNT(*) FROM raw_events WHERE received_at < now() - interval '60 days') AS raw_events_older_60d
                """
            )
        )
        row = counts.one()
        print(
            f'raw_events_total={row.raw_events_total} '
            f'raw_events_archive_total={row.raw_events_archive_total} '
            f'raw_events_older_60d={row.raw_events_older_60d}'
        )


async def archive_and_cleanup(*, retention_days: int, batch_size: int, max_batches: int | None, dry_run: bool) -> MaintenanceResult:
    archived_total = 0
    deleted_total = 0
    batches = 0

    while True:
        if max_batches is not None and batches >= max_batches:
            break

        async with AsyncSessionLocal() as session:
            candidates = await session.execute(
                text(
                    """
                    WITH candidate_ids AS (
                        SELECT id
                        FROM raw_events
                        WHERE received_at < now() - (:retention_days * interval '1 day')
                        ORDER BY id
                        LIMIT :batch_size
                    )
                    SELECT COUNT(*)
                    FROM candidate_ids
                    """
                ),
                {'retention_days': retention_days, 'batch_size': batch_size},
            )
            candidate_count = int(candidates.scalar_one())
            if candidate_count == 0:
                break

            if dry_run:
                archived_total += candidate_count
                deleted_total += candidate_count
                batches += 1
                continue

            inserted = await session.execute(
                text(
                    """
                    WITH candidate_ids AS (
                        SELECT id
                        FROM raw_events
                        WHERE received_at < now() - (:retention_days * interval '1 day')
                        ORDER BY id
                        LIMIT :batch_size
                    )
                    INSERT INTO raw_events_archive (
                        id,
                        stream_id,
                        source,
                        payload_type,
                        gufi,
                        flight_id,
                        raw_xml,
                        parsed_json,
                        received_at
                    )
                    SELECT
                        r.id,
                        r.stream_id,
                        r.source,
                        r.payload_type,
                        r.gufi,
                        r.flight_id,
                        r.raw_xml,
                        r.parsed_json,
                        r.received_at
                    FROM raw_events r
                    JOIN candidate_ids c ON c.id = r.id
                    ON CONFLICT (id) DO NOTHING
                    RETURNING id
                    """
                ),
                {'retention_days': retention_days, 'batch_size': batch_size},
            )
            inserted_count = len(inserted.fetchall())

            deleted = await session.execute(
                text(
                    """
                    WITH archived_ids AS (
                        SELECT id
                        FROM raw_events_archive
                        WHERE received_at < now() - (:retention_days * interval '1 day')
                        ORDER BY id
                        LIMIT :batch_size
                    )
                    DELETE FROM raw_events r
                    USING archived_ids a
                    WHERE r.id = a.id
                    RETURNING r.id
                    """
                ),
                {'retention_days': retention_days, 'batch_size': batch_size},
            )
            deleted_count = len(deleted.fetchall())

            await session.commit()
            archived_total += inserted_count
            deleted_total += deleted_count
            batches += 1

    return MaintenanceResult(archived=archived_total, deleted=deleted_total)


async def vacuum_analyze() -> None:
    autocommit_engine = sfdps_engine.execution_options(isolation_level='AUTOCOMMIT')
    async with autocommit_engine.connect() as conn:
        try:
            await conn.execute(text('VACUUM (ANALYZE) raw_events'))
        except Exception as exc:
            print(f'vacuum_warning_raw_events={exc}')
        try:
            await conn.execute(text('VACUUM (ANALYZE) raw_events_archive'))
        except Exception as exc:
            print(f'vacuum_warning_raw_events_archive={exc}')


async def main(args: argparse.Namespace) -> None:
    if args.command == 'baseline':
        await baseline()
        return
    if args.command == 'archive':
        result = await archive_and_cleanup(
            retention_days=args.retention_days,
            batch_size=args.batch_size,
            max_batches=args.max_batches,
            dry_run=args.dry_run,
        )
        print(f'archived={result.archived} deleted={result.deleted} dry_run={args.dry_run}')
        return
    if args.command == 'vacuum':
        await vacuum_analyze()
        print('vacuum_done=true')
        return
    raise ValueError(f'Unknown command: {args.command}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='SFDPS storage maintenance utilities')
    sub = parser.add_subparsers(dest='command', required=True)

    sub.add_parser('baseline', help='Print table sizes and retention counters')

    archive_parser = sub.add_parser('archive', help='Archive and delete old raw_events rows')
    archive_parser.add_argument('--retention-days', type=int, default=60)
    archive_parser.add_argument('--batch-size', type=int, default=10000)
    archive_parser.add_argument('--max-batches', type=int, default=None)
    archive_parser.add_argument('--dry-run', action='store_true')

    sub.add_parser('vacuum', help='Run VACUUM ANALYZE on raw event tables')

    asyncio.run(main(parser.parse_args()))
