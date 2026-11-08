"""Only allowlisted error messages cross the public boundary."""

MESSAGES = {
    "auth": "XMLRiver credentials are missing or rejected.",
    "balance": "XMLRiver balance is insufficient.",
    "ip": "XMLRiver rejected this IP address.",
    "params": "Invalid request parameters.",
    "temp": "XMLRiver is temporarily unavailable.",
    "format": "XMLRiver returned an unexpected response format.",
    "budget": "HTTP attempt limit reached.",
    "deadline": "Request deadline exceeded.",
    "other": "The operation failed.",
}


class XmlRiverError(RuntimeError):
    def __init__(self, message: str = "", *, code: str | None = None, kind: str = "other"):
        # Never expose arbitrary provider text, URLs or exception strings.
        self.kind = kind if kind in MESSAGES else "other"
        self.code = code if code and code.isdecimal() and len(code) < 6 else None
        super().__init__(MESSAGES[self.kind])

    def payload(self) -> dict:
        return {
            "kind": self.kind,
            "code": self.code,
            "message": str(self),
            "retryable": self.kind in {"temp", "deadline"},
        }
