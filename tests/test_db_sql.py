from __future__ import annotations

from pathlib import Path

import pytest

from pipelines.tasks import db as db_module


def test_render_sql_template_rejects_unsafe_identifier(tmp_path: Path):
    template = tmp_path / "x.sql"
    template.write_text("create table {tbl} (id int);")

    with pytest.raises(ValueError, match="unsafe identifier"):
        db_module._render_sql_template(template, tbl="bad name; drop table users")


def test_render_sql_template_accepts_safe_identifier(tmp_path: Path):
    template = tmp_path / "x.sql"
    template.write_text("create table {tbl} (id int);\ncreate index idx_{tbl} on {tbl}(id);")

    statements = db_module._render_sql_template(template, tbl="respira_gold.runs")
    assert len(statements) == 2
    assert all("respira_gold.runs" in s for s in statements)


def test_split_sql_respects_quoted_semicolons():
    sql = "insert into t(v) values ('a;b'); create table t2 (id int);"
    statements = db_module._split_sql(sql)
    assert len(statements) == 2
    assert "a;b" in statements[0]
