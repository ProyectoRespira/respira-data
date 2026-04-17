from __future__ import annotations

import re


_DSN_CREDENTIALS_RE = re.compile(
    r"(?P<scheme>[a-zA-Z][a-zA-Z0-9+.\-]*://)(?P<user>[^:/\s@]+):(?P<password>[^@\s]+)@"
)


def redact_dsn(value: str) -> str:
    """Redact user:password@ credentials embedded in any DSN-like substring.

    Matches ``scheme://user:password@host`` and replaces it with
    ``scheme://***:***@host``. Operates on arbitrary strings so it can safely
    be applied to exception messages, SQLAlchemy errors, or log lines that
    may contain connection strings.
    """

    if not value:
        return value
    return _DSN_CREDENTIALS_RE.sub(r"\g<scheme>***:***@", value)


def safe_error_str(exc: BaseException, *, max_len: int = 1000) -> str:
    """Stringify an exception with credentials redacted and length bounded."""

    return redact_dsn(str(exc))[:max_len]
