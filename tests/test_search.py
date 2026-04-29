import sqlite3
from pathlib import Path

import pytest

from capsule.indexer import init_db, index_file
from capsule.search import search_symbols


SAMPLE_PY = '''\
def autenticar_token(token: str) -> bool:
    """Verifica se o token Bearer é válido."""
    return token.startswith("Bearer ")


def listar_usuarios() -> list:
    """Retorna todos os usuários cadastrados."""
    return []
'''


@pytest.fixture
def populated_conn(tmp_path: Path) -> sqlite3.Connection:
    src = tmp_path / "api.py"
    src.write_text(SAMPLE_PY, encoding="utf-8")
    conn = init_db(tmp_path / ".capsule" / "index.db")
    index_file(conn, src)
    return conn


def test_search_by_name(populated_conn: sqlite3.Connection) -> None:
    results = search_symbols(populated_conn, "autenticar")
    assert any(s.name == "autenticar_token" for s in results)


def test_search_by_docstring(populated_conn: sqlite3.Connection) -> None:
    results = search_symbols(populated_conn, "Bearer")
    assert any("autenticar" in s.name for s in results)


def test_search_no_match(populated_conn: sqlite3.Connection) -> None:
    results = search_symbols(populated_conn, "xyz_inexistente_9999")
    assert results == []


def test_search_name_ranked_before_docstring(populated_conn: sqlite3.Connection) -> None:
    results = search_symbols(populated_conn, "token")
    # "autenticar_token" has "token" in name; should appear before docstring-only match
    assert results[0].name == "autenticar_token"


def test_search_max_results(populated_conn: sqlite3.Connection) -> None:
    results = search_symbols(populated_conn, "a", max_results=1)
    assert len(results) <= 1
