import sqlite3
from pathlib import Path

import pytest

from capsule.indexer import init_db, index_file


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
