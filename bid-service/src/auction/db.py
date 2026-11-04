"""Pool do Postgres e migrations versionadas (V1__x.sql, V2__y.sql), aplicadas no startup.

Várias tasks sobem ao mesmo tempo: um advisory lock garante que só uma aplica as migrations.
"""

import logging
import re
from pathlib import Path

import asyncpg

log = logging.getLogger(__name__)

MIGRATIONS = Path(__file__).parent / "migrations"
_LOCK_ID = 727_001  # identificador arbitrário do advisory lock das migrations


async def create_pool(dsn: str, size: int) -> asyncpg.Pool:
    return await asyncpg.create_pool(dsn, min_size=1, max_size=size, command_timeout=10)


async def migrate(pool: asyncpg.Pool, directory: Path = MIGRATIONS) -> None:
    files = sorted(directory.glob("V*__*.sql"), key=lambda p: int(re.match(r"V(\d+)__", p.name).group(1)))
    async with pool.acquire() as conn:
        await conn.execute("select pg_advisory_lock($1)", _LOCK_ID)
        try:
            await conn.execute("""
                create table if not exists schema_migrations (
                    version    int primary key,
                    name       text not null,
                    applied_at timestamptz not null default now())
                """)
            applied = {r["version"] for r in await conn.fetch("select version from schema_migrations")}
            for f in files:
                version = int(re.match(r"V(\d+)__", f.name).group(1))
                if version in applied:
                    continue
                async with conn.transaction():
                    await conn.execute(f.read_text())
                    await conn.execute("insert into schema_migrations (version, name) values ($1, $2)", version, f.name)
                log.info("migration aplicada: %s", f.name)
        finally:
            await conn.execute("select pg_advisory_unlock($1)", _LOCK_ID)
