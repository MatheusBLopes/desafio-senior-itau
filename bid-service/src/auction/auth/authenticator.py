"""Autenticação: quem é o usuário de cada lance.

Dois modos (AUTH_MODE):
- cognito (AWS): o cliente manda `Authorization: Bearer <access token>` emitido pelo Amazon Cognito. A task
  valida o JWT sozinha, com as chaves públicas do pool (JWKS) em cache: assinatura RS256, emissor, app client,
  tipo de token e validade. O usuário do lance é o `username` do token; o header X-User-Id é ignorado.
- header (local e testes de carga): o usuário vem do X-User-Id, sem prova de identidade. Só para desenvolvimento.

Cadastro e login passam pela API (POST /auth/signup e /auth/login), que chama o Cognito. O cadastro é confirmado
na hora com AdminConfirmSignUp (permissão IAM da task): sem e-mail, SMS ou Lambda.
"""

import logging
import re
from typing import Any

from ..config import Settings
from ..core.errors import ApiError
from .cognito import CognitoIdentityProvider, IdentityProvider
from .tokens import USER_ID, CognitoConfig, HttpJwks, JwtVerifier

log = logging.getLogger(__name__)

USERNAME = re.compile(r"^[A-Za-z0-9_-]{3,64}$")


class Authenticator:
    """Resolve o usuário de uma requisição conforme o modo configurado."""

    def __init__(self, mode: str, verifier: JwtVerifier | None = None, identity: IdentityProvider | None = None):
        """Valida a combinação: o modo cognito exige o verificador de JWT e o provedor de identidade."""
        if mode not in ("cognito", "header"):
            raise ValueError(f"AUTH_MODE desconhecido: {mode}")
        if mode == "cognito" and (verifier is None or identity is None):
            raise ValueError("AUTH_MODE=cognito exige o verificador de JWT e o provedor de identidade")
        self.mode = mode
        self.verifier = verifier
        self.identity = identity

    async def user_of(self, headers: Any) -> str:
        """Usuário da requisição: do access token (cognito) ou do X-User-Id (header). Sem usuário válido, 400/401."""
        if self.mode == "header":
            user = headers.get("x-user-id") or ""
            if not USER_ID.match(user):
                raise ApiError.bad_request("X-User-Id: use 1 a 64 caracteres [A-Za-z0-9_-]")
            return user
        authorization = headers.get("authorization") or ""
        scheme, _, token = authorization.partition(" ")
        if scheme.lower() != "bearer" or not token:
            raise ApiError.unauthorized("envie Authorization: Bearer <access token>")
        return await self.verifier.user_of(token.strip())

    def identity_provider(self) -> IdentityProvider:
        """Provedor de cadastro e login; não existe no modo header (404)."""
        if self.identity is None:
            raise ApiError.not_found("cadastro e login só existem com AUTH_MODE=cognito")
        return self.identity


def create_authenticator(settings: Settings) -> Authenticator:
    """Monta o autenticador do modo configurado. No modo cognito, exige o pool e o app client (fail-closed)."""
    if settings.auth_mode != "cognito":
        log.warning("AUTH_MODE=%s: o usuário vem do X-User-Id, sem autenticação. Só para desenvolvimento.",
                    settings.auth_mode)
        return Authenticator(settings.auth_mode)
    if not settings.cognito_user_pool_id or not settings.cognito_client_id:
        raise RuntimeError("AUTH_MODE=cognito exige COGNITO_USER_POOL_ID e COGNITO_CLIENT_ID")
    config = CognitoConfig(settings.cognito_region, settings.cognito_user_pool_id, settings.cognito_client_id)
    verifier = JwtVerifier(config.issuer, config.client_id, HttpJwks(config.jwks_url))
    return Authenticator("cognito", verifier, CognitoIdentityProvider(config))


def validate_credentials(body: Any) -> tuple[str, str]:
    """Confere o corpo de /auth/signup e /auth/login e devolve (username, password)."""
    if not isinstance(body, dict):
        raise ApiError.bad_request('envie {"username": "...", "password": "..."}')
    username, password = body.get("username"), body.get("password")
    if not isinstance(username, str) or not USERNAME.match(username):
        raise ApiError.bad_request("username: use 3 a 64 caracteres [A-Za-z0-9_-]")
    if not isinstance(password, str) or not 8 <= len(password) <= 128:
        raise ApiError.bad_request("password: de 8 a 128 caracteres")
    return username, password
