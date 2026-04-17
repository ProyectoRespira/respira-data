from __future__ import annotations

from pipelines.tasks.redaction import redact_dsn, safe_error_str


def test_redact_dsn_postgres():
    dsn = "postgresql+psycopg://alice:hunter2@db.example.com:5432/warehouse?sslmode=require"
    redacted = redact_dsn(dsn)
    assert "hunter2" not in redacted
    assert "alice" not in redacted
    assert "db.example.com:5432" in redacted
    assert redacted.startswith("postgresql+psycopg://***:***@")


def test_redact_dsn_embedded_in_error_text():
    text = (
        "sqlalchemy.exc.OperationalError: "
        "(psycopg.OperationalError) connection to postgresql://bob:s3cret@1.2.3.4/db failed"
    )
    redacted = redact_dsn(text)
    assert "s3cret" not in redacted
    assert "bob" not in redacted
    assert "1.2.3.4/db" in redacted


def test_redact_dsn_passthrough_when_no_credentials():
    assert redact_dsn("just a plain message") == "just a plain message"
    assert redact_dsn("") == ""


def test_safe_error_str_truncates_and_redacts():
    exc = RuntimeError("postgresql://a:b@host/db " + ("x" * 2000))
    out = safe_error_str(exc, max_len=100)
    assert len(out) == 100
    assert "a:b" not in out
