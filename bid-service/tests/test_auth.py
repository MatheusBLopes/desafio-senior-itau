"""Autenticação com JWT do Cognito: o verificador (com chaves geradas aqui) e a API em modo cognito."""

import base64
import hashlib
import hmac
import json
import time
import uuid

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm

from auction.auth.authenticator import Authenticator
from auction.auth.tokens import JwtVerifier
from auction.core.errors import ApiError

from .conftest import key

ISSUER = "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_TEST"
CLIENT_ID = "test-client"


class KeyPair:
    def __init__(self, kid: str):
        self.kid = kid
        self.private = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    def jwk(self) -> dict:
        return {**json.loads(RSAAlgorithm.to_jwk(self.private.public_key())), "kid": self.kid, "alg": "RS256",
                "use": "sig"}

    def public_pem(self) -> bytes:
        return self.private.public_key().public_bytes(serialization.Encoding.PEM,
                                                      serialization.PublicFormat.SubjectPublicKeyInfo)

    def token(self, username: str = "ana", **overrides) -> str:
        now = int(time.time())
        claims = {"sub": str(uuid.uuid4()), "iss": ISSUER, "client_id": CLIENT_ID, "token_use": "access",
                  "username": username, "iat": now, "exp": now + 3600, **overrides}
        claims = {k: v for k, v in claims.items() if v is not None}
        return jwt.encode(claims, self.private, algorithm="RS256", headers={"kid": self.kid})


class FakeJwks:
    def __init__(self, *keys: KeyPair):
        self.keys = list(keys)
        self.fetches = 0

    async def fetch(self) -> dict:
        self.fetches += 1
        return {"keys": [k.jwk() for k in self.keys]}


@pytest.fixture(scope="module")
def signer() -> KeyPair:
    return KeyPair("kid-1")


def verifier_for(*keys: KeyPair) -> tuple[JwtVerifier, FakeJwks]:
    jwks = FakeJwks(*keys)
    return JwtVerifier(ISSUER, CLIENT_ID, jwks), jwks


async def rejected(verifier: JwtVerifier, token: str) -> str:
    with pytest.raises(ApiError) as e:
        await verifier.user_of(token)
    assert e.value.status == 401
    return e.value.message


# --- Verificador ----------------------------------------------------------------------------------

async def test_valid_access_token_yields_the_username_and_is_cached(signer):
    verifier, jwks = verifier_for(signer)
    token = signer.token("ana")
    assert await verifier.user_of(token) == "ana"
    assert await verifier.user_of(token) == "ana"
    assert jwks.fetches == 1  # o JWKS é buscado uma vez e as chaves ficam em cache


async def test_expired_wrong_issuer_wrong_client_and_id_token_are_refused(signer):
    verifier, _ = verifier_for(signer)
    assert "expirado" in await rejected(verifier, signer.token(exp=int(time.time()) - 60))
    assert "inválido" in await rejected(verifier, signer.token(iss="https://evil.example.com/pool"))
    assert "inválido" in await rejected(verifier, signer.token(client_id="other-client"))
    assert "access token" in await rejected(verifier, signer.token(token_use="id"))
    assert "inválido" in await rejected(verifier, signer.token(username="ana:admin"))
    assert "inválido" in await rejected(verifier, signer.token(username=None))


async def test_algorithm_confusion_and_unsigned_tokens_are_refused(signer):
    verifier, _ = verifier_for(signer)
    claims = jwt.decode(signer.token(), options={"verify_signature": False})
    # alg=none: sem assinatura nenhuma.
    header = base64.urlsafe_b64encode(json.dumps({"alg": "none", "kid": signer.kid}).encode()).rstrip(b"=")
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).rstrip(b"=")
    assert "algoritmo" in await rejected(verifier, f"{header.decode()}.{payload.decode()}.")
    # HS256 "assinado" com a chave pública como segredo (ataque clássico de confusão de algoritmo).
    # Montado à mão, como faria um atacante: a própria PyJWT se recusa a gerá-lo.
    def b64(raw: bytes) -> str:
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

    signing_input = f"{b64(json.dumps({'alg': 'HS256', 'kid': signer.kid}).encode())}.{b64(json.dumps(claims).encode())}"
    mac = hmac.new(signer.public_pem(), signing_input.encode(), hashlib.sha256).digest()
    assert "algoritmo" in await rejected(verifier, f"{signing_input}.{b64(mac)}")


async def test_tampered_payload_and_foreign_key_are_refused(signer):
    verifier, _ = verifier_for(signer)
    head, body, sig = signer.token("ana").split(".")
    claims = json.loads(base64.urlsafe_b64decode(body + "=="))
    claims["username"] = "admin"
    evil_body = base64.urlsafe_b64encode(json.dumps(claims).encode()).rstrip(b"=").decode()
    assert "inválido" in await rejected(verifier, f"{head}.{evil_body}.{sig}")
    # Assinado por outra chave com o mesmo kid.
    impostor = KeyPair(signer.kid)
    assert "inválido" in await rejected(verifier, impostor.token("ana"))
    assert "malformado" in await rejected(verifier, "isto-nao-e-um-jwt")


async def test_cognito_unreachable_is_a_503_not_a_bad_token(signer):
    class Down:
        async def fetch(self) -> dict:
            raise OSError("cognito fora do ar")

    verifier = JwtVerifier(ISSUER, CLIENT_ID, Down())
    with pytest.raises(ApiError) as e:
        await verifier.user_of(signer.token("ana"))
    assert e.value.status == 503


async def test_key_rotation_refetches_the_jwks_but_unknown_kids_cannot_flood_it(signer):
    verifier, jwks = verifier_for(signer)
    assert await verifier.user_of(signer.token("ana")) == "ana"
    rotated = KeyPair("kid-2")
    jwks.keys.append(rotated)
    verifier._last_refresh = float("-inf")  # passou o intervalo mínimo entre buscas
    assert await verifier.user_of(rotated.token("bia")) == "bia"
    fetches = jwks.fetches
    for _ in range(20):
        await rejected(verifier, KeyPair(f"kid-{uuid.uuid4()}").token())
    assert jwks.fetches <= fetches + 1


# --- API em modo cognito --------------------------------------------------------------------------

class FakeIdentity:
    """Faz o papel do Cognito: guarda usuários e emite access tokens assinados pela chave de teste."""

    def __init__(self, signer: KeyPair):
        self.signer = signer
        self.users: dict[str, str] = {}

    async def sign_up(self, username: str, password: str) -> None:
        if username in self.users:
            raise ApiError(409, "USERNAME_TAKEN", "nome de usuário já cadastrado")
        self.users[username] = password

    async def login(self, username: str, password: str) -> dict:
        if self.users.get(username) != password:
            raise ApiError(401, "UNAUTHORIZED", "usuário ou senha inválidos")
        return {"accessToken": self.signer.token(username), "tokenType": "Bearer", "expiresIn": 3600}


@pytest.fixture
async def cognito_mode(server, signer):
    original = server.c.auth
    verifier, _ = verifier_for(signer)
    server.c.auth = Authenticator("cognito", verifier, FakeIdentity(signer))
    yield
    server.c.auth = original


async def test_signup_login_and_bid_with_the_token(server, http, cognito_mode):
    user, password = f"u{uuid.uuid4().hex[:10]}", "senha-forte-123"
    assert (await http.post("/auth/signup", json={"username": user, "password": password})).status_code == 201
    assert (await http.post("/auth/signup", json={"username": user, "password": password})).status_code == 409
    assert (await http.post("/auth/signup", json={"username": "a:b", "password": password})).status_code == 400
    assert (await http.post("/auth/login", json={"username": user, "password": "errada-123"})).status_code == 401
    token = (await http.post("/auth/login", json={"username": user, "password": password})).json()["accessToken"]
    bearer = {"Authorization": f"Bearer {token}"}

    created = await http.post("/auctions", headers=bearer, json={
        "title": "com login", "startingPrice": "100.00", "minIncrement": "1.00", "durationSeconds": 60})
    assert created.status_code == 201
    aid = created.json()["id"]

    # O X-User-Id é ignorado: quem dá o lance é o dono do token.
    bid = await http.post(f"/auctions/{aid}/bids", json={"amount": "100.00"},
                          headers={**bearer, "Idempotency-Key": key(), "X-User-Id": "outra-pessoa"})
    assert bid.status_code == 201
    assert (await http.get(f"/auctions/{aid}")).json()["leaderId"] == user


async def test_bids_and_auction_creation_require_a_valid_token(server, http, cognito_mode, signer):
    created = await http.post("/auctions", json={
        "title": "x", "startingPrice": "100.00", "minIncrement": "1.00", "durationSeconds": 60})
    assert created.status_code == 401
    # Sem token, nem o formato do corpo é revelado: 401 antes da validação.
    assert (await http.post("/auctions", json={})).status_code == 401

    from .conftest import new_auction
    aid = await new_auction(server.c, 60)
    url = f"/auctions/{aid}/bids"
    body = {"amount": "100.00"}
    assert (await http.post(url, json=body, headers={"Idempotency-Key": key(), "X-User-Id": "ana"})).status_code == 401
    assert (await http.post(url, json=body, headers={"Idempotency-Key": key(),
                                                     "Authorization": "Bearer lixo"})).status_code == 401
    expired = signer.token("ana", exp=int(time.time()) - 60)
    response = await http.post(url, json=body, headers={"Idempotency-Key": key(), "Authorization": f"Bearer {expired}"})
    assert response.status_code == 401 and response.json()["code"] == "UNAUTHORIZED"
    # Leitura continua pública: o estado do leilão não é segredo.
    assert (await http.get(f"/auctions/{aid}")).status_code == 200


async def test_signup_and_login_do_not_exist_in_header_mode(http):
    response = await http.post("/auth/login", json={"username": "ana", "password": "12345678"})
    assert response.status_code == 404
