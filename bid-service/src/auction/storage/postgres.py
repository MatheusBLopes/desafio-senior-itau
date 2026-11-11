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
_VERSION = re.compile(r"V(\d+)__")


async def create_pool(dsn: str, size: int) -> asyncpg.Pool:
    """Pool de conexões com timeouts curtos, para que um banco fora do ar vire 503 rápido."""
    # timeout = conexão nova: um banco parado ou inalcançável não responde (sem RST), e o padrão de 60 s
    # empataria com o idle timeout do ALB (504). Com 3 s, a requisição vira 503 rápido (medido na AWS).
    return await asyncpg.create_pool(dsn, min_size=1, max_size=size, timeout=3, command_timeout=10)


def _version_of(migration: Path) -> int:
    """Número da migration a partir do nome do arquivo (V12__x.sql → 12)."""
    return int(_VERSION.match(migration.name).group(1))


async def migrate(pool: asyncpg.Pool, directory: Path = MIGRATIONS) -> None:
    """Aplica, em ordem e cada uma numa transação, as migrations que ainda não estão em schema_migrations."""
    migrations = sorted(directory.glob("V*__*.sql"), key=_version_of)
    async with pool.acquire() as conn:
        await conn.execute("select pg_advisory_lock($1)", _LOCK_ID)
        try:
            await conn.execute("""
                create table if not exists schema_migrations (
                    version    int primary key,
                    name       text not null,
                    applied_at timestamptz not null default now())
                """)
            applied = {row["version"] for row in await conn.fetch("select version from schema_migrations")}
            for migration in migrations:
                version = _version_of(migration)
                if version in applied:
                    continue
                async with conn.transaction():
                    await conn.execute(migration.read_text())
                    await conn.execute("insert into schema_migrations (version, name) values ($1, $2)",
                                       version, migration.name)
                log.info("migration aplicada: %s", migration.name)
        finally:
            await conn.execute("select pg_advisory_unlock($1)", _LOCK_ID)
