"""Erro de API: carrega o status HTTP e o código que o cliente recebe no corpo da resposta."""


class ApiError(Exception):
    """Erro esperado de negócio ou de validação; o handler da app o converte em {"code", "message"}."""

    def __init__(self, status: int, code: str, message: str):
        """Status HTTP, código estável (ex.: BID_TOO_LOW) e mensagem para o cliente."""
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message

    @staticmethod
    def not_found(message: str) -> "ApiError":
        """404: o recurso pedido (em geral, um leilão) não existe."""
        return ApiError(404, "NOT_FOUND", message)

    @staticmethod
    def bad_request(message: str) -> "ApiError":
        """400: o corpo ou um header não passou na validação."""
        return ApiError(400, "INVALID_REQUEST", message)

    @staticmethod
    def unauthorized(message: str) -> "ApiError":
        """401: sem token, ou token inválido, expirado ou de outro app client."""
        return ApiError(401, "UNAUTHORIZED", message)

    @staticmethod
    def unavailable(message: str = "tente de novo em instantes") -> "ApiError":
        """503: uma dependência está fora do ar; o cliente pode tentar de novo."""
        return ApiError(503, "UNAVAILABLE", message)
