import sqlite3
from pathlib import Path

import pytest

from capsule.indexer import init_db, index_file
from capsule.ts_parser import TS_AVAILABLE


def test_migration_adds_language_column(tmp_path: Path) -> None:
    db_path = tmp_path / ".capsule" / "index.db"
    db_path.parent.mkdir(parents=True)
    conn = sqlite3.connect(str(db_path))
    conn.executescript("""
        CREATE TABLE files (id INTEGER PRIMARY KEY, path TEXT UNIQUE, hash TEXT, indexed_at REAL);
        CREATE TABLE symbols (id INTEGER PRIMARY KEY, file_id INTEGER, name TEXT, kind TEXT,
            start_line INTEGER, end_line INTEGER, signature TEXT, docstring TEXT, body TEXT);
        CREATE TABLE imports (id INTEGER PRIMARY KEY, file_id INTEGER, module TEXT, alias TEXT);
    """)
    conn.execute("INSERT INTO files (path, hash, indexed_at) VALUES ('x.py', 'abc', 0.0)")
    conn.commit()
    conn.close()

    conn2 = init_db(db_path)
    cols = {c[1] for c in conn2.execute("PRAGMA table_info(symbols)").fetchall()}
    assert "language" in cols

    imp_cols = {c[1] for c in conn2.execute("PRAGMA table_info(imports)").fetchall()}
    assert "symbol" in imp_cols

    count = conn2.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    assert count == 0

    # FTS5 virtual table was created
    tables = {r[0] for r in conn2.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert "symbols_fts" in tables


def test_migration_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / ".capsule" / "index.db"
    conn = init_db(db_path)
    # Insert a sentinel file — must survive second init_db call (no migration on up-to-date schema)
    conn.execute("INSERT INTO files (path, hash, indexed_at) VALUES ('sentinel.py', 'hash', 0.0)")
    conn.commit()
    conn.close()

    conn2 = init_db(db_path)
    cols = {c[1] for c in conn2.execute("PRAGMA table_info(symbols)").fetchall()}
    assert "language" in cols
    imp_cols = {c[1] for c in conn2.execute("PRAGMA table_info(imports)").fetchall()}
    assert "symbol" in imp_cols
    # sentinel file must still exist — no migration triggered, files not cleared
    count = conn2.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    assert count == 1


def test_import_from_stores_symbol(tmp_path: Path) -> None:
    src = tmp_path / "consumer.py"
    src.write_text("from api import autenticar_token\n", encoding="utf-8")
    conn = init_db(tmp_path / ".capsule" / "index.db")
    index_file(conn, src)
    row = conn.execute("SELECT symbol FROM imports WHERE module = 'api'").fetchone()
    assert row is not None
    assert row[0] == "autenticar_token"


def test_import_from_aliased_stores_symbol(tmp_path: Path) -> None:
    src = tmp_path / "consumer.py"
    src.write_text("from api import autenticar_token as auth\n", encoding="utf-8")
    conn = init_db(tmp_path / ".capsule" / "index.db")
    index_file(conn, src)
    row = conn.execute("SELECT symbol, alias FROM imports WHERE module = 'api'").fetchone()
    assert row is not None
    assert row[0] == "autenticar_token"
    assert row[1] == "auth"


SAMPLE_TS = """\
function greet(name: string): string {
    return `Hello, ${name}`;
}

interface User {
    id: number;
    name: string;
}

type UserId = number;
"""


def test_typescript_indexing(tmp_path: Path) -> None:
    if not TS_AVAILABLE:
        pytest.skip("tree-sitter-typescript not installed")
    src = tmp_path / "sample.ts"
    src.write_text(SAMPLE_TS, encoding="utf-8")
    conn = init_db(tmp_path / ".capsule" / "index.db")
    index_file(conn, src)
    rows = conn.execute("SELECT name, kind, language FROM symbols").fetchall()
    by_name = {r[0]: (r[1], r[2]) for r in rows}
    assert "greet" in by_name
    assert by_name["greet"] == ("function", "typescript")
    assert "User" in by_name
    assert by_name["User"][0] == "interface"
    assert "UserId" in by_name
    assert by_name["UserId"][0] == "type"


def test_fts5_populated_after_index(tmp_path: Path) -> None:
    src = tmp_path / "api.py"
    src.write_text(
        'def autenticar_token(token: str) -> bool:\n'
        '    """Verifica se o token Bearer é válido."""\n'
        '    return True\n',
        encoding="utf-8",
    )
    conn = init_db(tmp_path / ".capsule" / "index.db")
    index_file(conn, src)
    count = conn.execute("SELECT COUNT(*) FROM symbols_fts").fetchone()[0]
    assert count > 0
