"""Validação local dos access tokens (JWT) do Amazon Cognito, sem chamar o Cognito a cada lance."""

import asyncio
import logging
import re
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Protocol

import httpx
import jwt
from jwt.algorithms import RSAAlgorithm

from ..core.errors import ApiError
from ..observability.logs import bind, fields

log = logging.getLogger(__name__)

# Sem ':' no usuário: ele compõe a chave idem:{auction}:<user>:<key> e não pode gerar ambiguidade.
USER_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


@dataclass(frozen=True)
class CognitoConfig:
    """Identifica o user pool e o app client cujos tokens são aceitos."""

    region: str
    user_pool_id: str
    client_id: str

    @property
    def issuer(self) -> str:
        """Valor esperado do claim iss nos tokens deste pool."""
        return f"https://cognito-idp.{self.region}.amazonaws.com/{self.user_pool_id}"

    @property
    def jwks_url(self) -> str:
        """Endereço público das chaves de assinatura do pool."""
        return f"{self.issuer}/.well-known/jwks.json"


class JwksSource(Protocol):
    """De onde vêm as chaves públicas; nos testes, uma fonte falsa entra no lugar do HTTP."""

    async def fetch(self) -> dict[str, Any]:
        """Devolve o documento JWKS ({"keys": [...]})."""
        ...


class HttpJwks:
    """Busca o JWKS público do pool. As chaves mudam raramente (rotação), então ficam em cache no verificador."""

    def __init__(self, url: str):
        """Endereço do JWKS do pool."""
        self._url = url

    async def fetch(self) -> dict[str, Any]:
        """GET no endereço do JWKS, com timeout de 5 s."""
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.get(self._url)
            response.raise_for_status()
            return response.json()


class JwtVerifier:
    """Valida access tokens do Cognito sem chamar o Cognito a cada requisição.

    - Só RS256 e só chaves do JWKS do pool: um token assinado com HS256 usando a chave pública como segredo
      (ataque de confusão de algoritmo) ou com alg=none é recusado.
    - kid desconhecido → busca o JWKS de novo (rotação de chaves), no máximo uma vez a cada 30 s, para um
      atacante não transformar tokens com kid inventado em uma enxurrada de requisições ao Cognito.
    - Cache de tokens já validados até o exp: um usuário manda muitos lances com o mesmo token, e verificar
      RSA a cada um gastaria CPU à toa. Tamanho limitado (LRU).
    """

    REFRESH_INTERVAL_S = 30
    LEEWAY_S = 5
    CACHE_SIZE = 10_000

    def __init__(self, issuer: str, client_id: str, jwks: JwksSource, clock: Any = time.time):
        """Emissor e app client esperados, fonte das chaves e relógio (injetável nos testes)."""
        self._issuer = issuer
        self._client_id = client_id
        self._jwks = jwks
        self._clock = clock
        self._keys: dict[str, Any] = {}
        self._last_refresh = float("-inf")
        self._fetch_failed = False
        self._refresh_lock = asyncio.Lock()
        self._verified: OrderedDict[str, tuple[str, float]] = OrderedDict()

    async def user_of(self, token: str) -> str:
        """Devolve o username do token, usando o cache quando o mesmo token já foi validado e não expirou."""
        cached = self._verified.get(token)
        now = self._clock()
        if cached is not None and cached[1] > now:
            self._verified.move_to_end(token)
            return cached[0]
        claims = await self._verify(token)
        user = claims["username"]
        self._verified[token] = (user, claims["exp"])
        if len(self._verified) > self.CACHE_SIZE:
            self._verified.popitem(last=False)
        return user

    async def _verify(self, token: str) -> dict[str, Any]:
        """Confere algoritmo, assinatura, emissor, validade, tipo de token e app client; devolve os claims."""
        try:
            header = jwt.get_unverified_header(token)
        except jwt.PyJWTError:
            raise ApiError.unauthorized("token malformado") from None
        if header.get("alg") != "RS256":
            raise ApiError.unauthorized("algoritmo de assinatura não aceito")
        key = await self._key(header.get("kid"))
        try:
            claims = jwt.decode(
                token, key=key, algorithms=["RS256"], issuer=self._issuer, leeway=self.LEEWAY_S,
                options={"require": ["exp", "iat", "iss", "token_use", "client_id", "username"],
                         "verify_aud": False})  # access token do Cognito não tem aud; o app client vem em client_id
        except jwt.ExpiredSignatureError:
            raise ApiError.unauthorized("token expirado") from None
        except jwt.PyJWTError as e:
            bind(authError=str(e))  # o detalhe vai para o log da requisição; o cliente recebe uma mensagem genérica
            raise ApiError.unauthorized("token inválido") from None
        if claims["token_use"] != "access":
            raise ApiError.unauthorized("use o access token, não o id token")
        if claims["client_id"] != self._client_id or not USER_ID.match(str(claims["username"])):
            bind(authError="client_id ou username inesperado")
            raise ApiError.unauthorized("token inválido")
        return claims

    async def _key(self, kid: str | None) -> Any:
        """Chave pública do kid; rebusca o JWKS se o kid é desconhecido (rotação de chaves)."""
        if not kid:
            raise ApiError.unauthorized("token sem kid")
        if kid not in self._keys:
            await self._refresh()
        key = self._keys.get(kid)
        if key is None:
            if not self._keys and self._fetch_failed:
                # Sem nenhuma chave porque o Cognito não respondeu: o problema é nosso, não do token.
                raise ApiError.unavailable("não foi possível validar o token agora")
            raise ApiError.unauthorized("token inválido")
        return key

    async def _refresh(self) -> None:
        """Rebusca o JWKS no máximo uma vez a cada REFRESH_INTERVAL_S; numa falha, permite tentar de novo logo."""
        async with self._refresh_lock:
            if self._clock() - self._last_refresh < self.REFRESH_INTERVAL_S:
                return
            self._last_refresh = self._clock()
            try:
                jwks = await self._jwks.fetch()
            except Exception as e:
                log.warning("falha ao buscar as chaves do Cognito (JWKS)", extra=fields(error=e))
                self._fetch_failed = True
                self._last_refresh = float("-inf")  # tenta de novo na próxima requisição
                return
            self._fetch_failed = False
            self._keys = {jwk["kid"]: RSAAlgorithm.from_jwk(jwk) for jwk in jwks.get("keys", [])
                          if jwk.get("kty") == "RSA" and jwk.get("kid")}
