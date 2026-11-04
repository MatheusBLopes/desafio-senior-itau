"""Scripts Lua registrados no cliente do Valkey.

register_script calcula o SHA1 do texto e cada chamada envia só EVALSHA <sha> + parâmetros. Se o
Valkey responder NOSCRIPT (cache de scripts perdido num restart), o redis-py carrega o texto
(SCRIPT LOAD) e repete: o código Lua trafega uma vez, não a cada lance.
"""

from dataclasses import dataclass
from pathlib import Path

from redis.asyncio import Redis
from redis.commands.core import AsyncScript

_DIR = Path(__file__).parent / "lua"


@dataclass(frozen=True)
class LuaScripts:
    place_bid: AsyncScript
    close_auction: AsyncScript
    hydrate_auction: AsyncScript
    rate_limit: AsyncScript

    @staticmethod
    def register(redis: Redis) -> "LuaScripts":
        def load(name: str) -> AsyncScript:
            return redis.register_script((_DIR / f"{name}.lua").read_text())

        return LuaScripts(load("place_bid"), load("close_auction"), load("hydrate_auction"), load("rate_limit"))
