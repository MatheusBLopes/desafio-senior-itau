"""Cadastro e login no Amazon Cognito (fora do caminho do lance)."""

import asyncio
import logging
from typing import Any, Protocol

from ..core.errors import ApiError
from .tokens import CognitoConfig

log = logging.getLogger(__name__)


class IdentityProvider(Protocol):
    """Quem cadastra e autentica usuários; nos testes, um provedor falso entra no lugar do Cognito."""

    async def sign_up(self, username: str, password: str) -> None:
        """Cria o usuário já confirmado."""
        ...

    async def login(self, username: str, password: str) -> dict[str, Any]:
        """Troca usuário e senha por {"accessToken", "tokenType", "expiresIn"}."""
        ...


class CognitoIdentityProvider:
    """Cadastro e login no Cognito. O boto3 é síncrono: roda numa thread para não travar o event loop.

    São operações raras (uma por usuário), fora do caminho do lance.
    """

    def __init__(self, config: CognitoConfig):
        """Cria o cliente boto3 do Cognito na região do pool."""
        import boto3  # só no modo cognito

        self._config = config
        self._client = boto3.client("cognito-idp", region_name=config.region)

    async def sign_up(self, username: str, password: str) -> None:
        """SignUp seguido de AdminConfirmSignUp: o usuário nasce confirmado, sem e-mail."""
        def run() -> None:
            """Chamada síncrona ao Cognito, executada numa thread."""
            self._client.sign_up(ClientId=self._config.client_id, Username=username, Password=password)
            # Confirma na hora: sem e-mail nem código. Exige cognito-idp:AdminConfirmSignUp na role da task.
            self._client.admin_confirm_sign_up(UserPoolId=self._config.user_pool_id, Username=username)

        await self._call(run)

    async def login(self, username: str, password: str) -> dict[str, Any]:
        """Fluxo USER_PASSWORD_AUTH; devolve o access token (1 h)."""
        def run() -> dict[str, Any]:
            """Chamada síncrona ao Cognito, executada numa thread."""
            result = self._client.initiate_auth(
                ClientId=self._config.client_id, AuthFlow="USER_PASSWORD_AUTH",
                AuthParameters={"USERNAME": username, "PASSWORD": password})
            return result["AuthenticationResult"]

        auth = await self._call(run)
        return {"accessToken": auth["AccessToken"], "tokenType": "Bearer", "expiresIn": auth["ExpiresIn"]}

    @staticmethod
    async def _call(operation: Any) -> Any:
        """Roda a chamada do boto3 numa thread e traduz os erros do Cognito em ApiError."""
        from botocore.exceptions import ClientError

        try:
            return await asyncio.to_thread(operation)
        except ClientError as e:
            raise cognito_error(e.response["Error"]["Code"], e.response["Error"].get("Message", "")) from None


def cognito_error(code: str, message: str) -> ApiError:
    """Traduz os erros do Cognito sem revelar se um usuário existe (login falho é sempre a mesma mensagem)."""
    if code == "UsernameExistsException":
        return ApiError(409, "USERNAME_TAKEN", "nome de usuário já cadastrado")
    if code in ("InvalidPasswordException", "InvalidParameterException"):
        return ApiError.bad_request(message or "senha fora da política")
    if code in ("NotAuthorizedException", "UserNotFoundException", "UserNotConfirmedException"):
        return ApiError.unauthorized("usuário ou senha inválidos")
    if code in ("TooManyRequestsException", "LimitExceededException", "TooManyFailedAttemptsException"):
        return ApiError(429, "RATE_LIMITED", "muitas tentativas, tente de novo em instantes")
    log.error("erro inesperado do Cognito: %s %s", code, message)
    return ApiError.unavailable("serviço de identidade indisponível")
