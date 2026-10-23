class DomainError(Exception):
    status_code = 400
    code = "bad_request"

    def __init__(self, detail: str, code: str | None = None) -> None:
        super().__init__(detail)
        self.detail = detail
        if code:
            self.code = code


class NotFound(DomainError):
    status_code = 404
    code = "not_found"


class Conflict(DomainError):
    status_code = 409
    code = "conflict"


class IdempotencyMismatch(DomainError):
    status_code = 422
    code = "idempotency_key_reuse"
