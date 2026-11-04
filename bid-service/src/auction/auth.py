"""Autenticação: quem é o usuário de cada lance.

Dois modos (AUTH_MODE):
- cognito (AWS): o cliente manda `Authorization: Bearer <access token>` emitido pelo Amazon Cognito. A task
  valida o JWT sozinha, com as chaves públicas do pool (JWKS) em cache: assinatura RS256, emissor, app client,
  tipo de token e validade. O usuário do lance é o `username` do token; o header X-User-Id é ignorado.
- header (local e testes de carga): o usuário vem do X-User-Id, sem prova de identidade. Só para desenvolvimento.

Cadastro e login passam pela API (POST /auth/signup e /auth/login), que chama o Cognito. O cadastro é confirmado
na hora com AdminConfirmSignUp (permissão IAM da task): sem e-mail, SMS ou Lambda.
"""

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

from .common import ApiError

log = logging.getLogger(__name__)

# Sem ':' no usuário: ele compõe a chave idem:{auction}:<user>:<key> e não pode gerar ambiguidade.
USER_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
USERNAME = re.compile(r"^[A-Za-z0-9_-]{3,64}$")


def unauthorized(message: str) -> ApiError:
    return ApiError(401, "UNAUTHORIZED", message)


@dataclass(frozen=True)
class CognitoConfig:
    region: str
    user_pool_id: str
    client_id: str

    @property
    def issuer(self) -> str:
        return f"https://cognito-idp.{self.region}.amazonaws.com/{self.user_pool_id}"

    @property
    def jwks_url(self) -> str:
        return f"{self.issuer}/.well-known/jwks.json"


class JwksSource(Protocol):
    async def fetch(self) -> dict[str, Any]: ...


class HttpJwks:
    """Busca o JWKS público do pool. As chaves mudam raramente (rotação), então ficam em cache no verificador."""

    def __init__(self, url: str):
        self._url = url

    async def fetch(self) -> dict[str, Any]:
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
        try:
            header = jwt.get_unverified_header(token)
        except jwt.PyJWTError:
            raise unauthorized("token malformado") from None
        if header.get("alg") != "RS256":
            raise unauthorized("algoritmo de assinatura não aceito")
        key = await self._key(header.get("kid"))
        try:
            claims = jwt.decode(
                token, key=key, algorithms=["RS256"], issuer=self._issuer, leeway=self.LEEWAY_S,
                options={"require": ["exp", "iat", "iss", "token_use", "client_id", "username"],
                         "verify_aud": False})  # access token do Cognito não tem aud; o app client vem em client_id
        except jwt.ExpiredSignatureError:
            raise unauthorized("token expirado") from None
        except jwt.PyJWTError as e:
            log.info("token recusado: %s", e)  # o detalhe fica no log; o cliente recebe uma mensagem genérica
            raise unauthorized("token inválido") from None
        if claims["token_use"] != "access":
            raise unauthorized("use o access token, não o id token")
        if claims["client_id"] != self._client_id or not USER_ID.match(str(claims["username"])):
            log.info("token recusado: client_id ou username inesperado")
            raise unauthorized("token inválido")
        return claims

    async def _key(self, kid: str | None) -> Any:
        if not kid:
            raise unauthorized("token sem kid")
        if kid not in self._keys:
            await self._refresh()
        key = self._keys.get(kid)
        if key is None:
            if not self._keys and self._fetch_failed:
                # Sem nenhuma chave porque o Cognito não respondeu: o problema é nosso, não do token.
                raise ApiError(503, "UNAVAILABLE", "não foi possível validar o token agora")
            raise unauthorized("token inválido")
        return key

    async def _refresh(self) -> None:
        async with self._refresh_lock:
            if self._clock() - self._last_refresh < self.REFRESH_INTERVAL_S:
                return
            self._last_refresh = self._clock()
            try:
                jwks = await self._jwks.fetch()
            except Exception as e:
                log.warning("falha ao buscar o JWKS do Cognito: %s", e)
                self._fetch_failed = True
                self._last_refresh = float("-inf")  # tenta de novo na próxima requisição
                return
            self._fetch_failed = False
            self._keys = {k["kid"]: RSAAlgorithm.from_jwk(k) for k in jwks.get("keys", [])
                          if k.get("kty") == "RSA" and k.get("kid")}


class IdentityProvider(Protocol):
    async def sign_up(self, username: str, password: str) -> None: ...

    async def login(self, username: str, password: str) -> dict[str, Any]: ...


class CognitoIdentityProvider:
    """Cadastro e login no Cognito. O boto3 é síncrono: roda numa thread para não travar o event loop.

    São operações raras (uma por usuário), fora do caminho do lance.
    """

    def __init__(self, config: CognitoConfig):
        import boto3  # só no modo cognito

        self._config = config
        self._client = boto3.client("cognito-idp", region_name=config.region)

    async def sign_up(self, username: str, password: str) -> None:
        def run() -> None:
            self._client.sign_up(ClientId=self._config.client_id, Username=username, Password=password)
            # Confirma na hora: sem e-mail nem código. Exige cognito-idp:AdminConfirmSignUp na role da task.
            self._client.admin_confirm_sign_up(UserPoolId=self._config.user_pool_id, Username=username)

        await self._call(run)

    async def login(self, username: str, password: str) -> dict[str, Any]:
        def run() -> dict[str, Any]:
            result = self._client.initiate_auth(
                ClientId=self._config.client_id, AuthFlow="USER_PASSWORD_AUTH",
                AuthParameters={"USERNAME": username, "PASSWORD": password})
            return result["AuthenticationResult"]

        auth = await self._call(run)
        return {"accessToken": auth["AccessToken"], "tokenType": "Bearer", "expiresIn": auth["ExpiresIn"]}

    @staticmethod
    async def _call(fn: Any) -> Any:
        from botocore.exceptions import ClientError

        try:
            return await asyncio.to_thread(fn)
        except ClientError as e:
            raise _cognito_error(e.response["Error"]["Code"], e.response["Error"].get("Message", "")) from None


def _cognito_error(code: str, message: str) -> ApiError:
    """Traduz os erros do Cognito sem revelar se um usuário existe (login falho é sempre a mesma mensagem)."""
    if code == "UsernameExistsException":
        return ApiError(409, "USERNAME_TAKEN", "nome de usuário já cadastrado")
    if code in ("InvalidPasswordException", "InvalidParameterException"):
        return ApiError.bad_request(message or "senha fora da política")
    if code in ("NotAuthorizedException", "UserNotFoundException", "UserNotConfirmedException"):
        return unauthorized("usuário ou senha inválidos")
    if code in ("TooManyRequestsException", "LimitExceededException", "TooManyFailedAttemptsException"):
        return ApiError(429, "RATE_LIMITED", "muitas tentativas, tente de novo em instantes")
    log.error("erro inesperado do Cognito: %s %s", code, message)
    return ApiError(503, "UNAVAILABLE", "serviço de identidade indisponível")


class Authenticator:
    """Resolve o usuário de uma requisição conforme o modo configurado."""

    def __init__(self, mode: str, verifier: JwtVerifier | None = None, identity: IdentityProvider | None = None):
        if mode not in ("cognito", "header"):
            raise ValueError(f"AUTH_MODE desconhecido: {mode}")
        if mode == "cognito" and (verifier is None or identity is None):
            raise ValueError("AUTH_MODE=cognito exige o verificador de JWT e o provedor de identidade")
        self.mode = mode
        self.verifier = verifier
        self.identity = identity

    async def user_of(self, headers: Any) -> str:
        if self.mode == "header":
            user = headers.get("x-user-id") or ""
            if not USER_ID.match(user):
                raise ApiError.bad_request("X-User-Id: use 1 a 64 caracteres [A-Za-z0-9_-]")
            return user
        authorization = headers.get("authorization") or ""
        scheme, _, token = authorization.partition(" ")
        if scheme.lower() != "bearer" or not token:
            raise unauthorized("envie Authorization: Bearer <access token>")
        return await self.verifier.user_of(token.strip())

    def identity_provider(self) -> IdentityProvider:
        if self.identity is None:
            raise ApiError.not_found("cadastro e login só existem com AUTH_MODE=cognito")
        return self.identity


def validate_credentials(body: Any) -> tuple[str, str]:
    if not isinstance(body, dict):
        raise ApiError.bad_request('envie {"username": "...", "password": "..."}')
    username, password = body.get("username"), body.get("password")
    if not isinstance(username, str) or not USERNAME.match(username):
        raise ApiError.bad_request("username: use 3 a 64 caracteres [A-Za-z0-9_-]")
    if not isinstance(password, str) or not 8 <= len(password) <= 128:
        raise ApiError.bad_request("password: de 8 a 128 caracteres")
    return username, password
