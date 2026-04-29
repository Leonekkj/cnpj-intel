import sqlite3
from pathlib import Path

import pytest

from capsule.indexer import init_db, index_file, is_stale


SAMPLE_PY = '''\
def autenticar_token(token: str) -> bool:
    """Verifica se o token Bearer é válido."""
    return token.startswith("Bearer ")


class GerenciadorDeTokens:
    """Gerencia tokens de autenticação."""

    def revogar(self, token: str) -> None:
        pass
'''


@pytest.fixture
def tmp_py(tmp_path: Path) -> Path:
    src = tmp_path / "sample.py"
    src.write_text(SAMPLE_PY, encoding="utf-8")
    return src


@pytest.fixture
def conn(tmp_path: Path) -> sqlite3.Connection:
    return init_db(tmp_path / ".capsule" / "index.db")


def test_index_file_extracts_function(tmp_py: Path, conn: sqlite3.Connection) -> None:
    index_file(conn, tmp_py)
    rows = conn.execute("SELECT name, kind FROM symbols WHERE name = 'autenticar_token'").fetchall()
    assert len(rows) == 1
    assert rows[0][1] == "function"


def test_index_file_extracts_class(tmp_py: Path, conn: sqlite3.Connection) -> None:
    index_file(conn, tmp_py)
    rows = conn.execute("SELECT name, kind FROM symbols WHERE name = 'GerenciadorDeTokens'").fetchall()
    assert len(rows) == 1
    assert rows[0][1] == "class"


def test_index_file_extracts_docstring(tmp_py: Path, conn: sqlite3.Connection) -> None:
    index_file(conn, tmp_py)
    row = conn.execute(
        "SELECT docstring FROM symbols WHERE name = 'autenticar_token'"
    ).fetchone()
    assert row is not None
    assert "token Bearer" in row[0]


def test_is_stale_new_file(tmp_py: Path, conn: sqlite3.Connection) -> None:
    assert is_stale(conn, tmp_py) is True


def test_is_stale_after_index(tmp_py: Path, conn: sqlite3.Connection) -> None:
    index_file(conn, tmp_py)
    assert is_stale(conn, tmp_py) is False


def test_is_stale_after_change(tmp_py: Path, conn: sqlite3.Connection) -> None:
    index_file(conn, tmp_py)
    tmp_py.write_text(SAMPLE_PY + "\n# changed", encoding="utf-8")
    assert is_stale(conn, tmp_py) is True


def test_idempotent_reindex(tmp_py: Path, conn: sqlite3.Connection) -> None:
    index_file(conn, tmp_py)
    index_file(conn, tmp_py)
    count = conn.execute("SELECT COUNT(*) FROM symbols WHERE name = 'autenticar_token'").fetchone()[0]
    assert count == 1
