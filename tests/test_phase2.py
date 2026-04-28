import sqlite3
from pathlib import Path

import pytest

from capsule.indexer import init_db, index_file


def test_migration_adds_language_column(tmp_path: Path) -> None:
    db_path = tmp_path / ".capsule" / "index.db"
    # Simulate old schema without language column
    db_path.parent.mkdir(parents=True)
    conn = sqlite3.connect(str(db_path))
    conn.executescript("""
        CREATE TABLE files (id INTEGER PRIMARY KEY, path TEXT UNIQUE, hash TEXT, indexed_at REAL);
        CREATE TABLE symbols (id INTEGER PRIMARY KEY, file_id INTEGER, name TEXT, kind TEXT,
            start_line INTEGER, end_line INTEGER, signature TEXT, docstring TEXT, body TEXT);
        CREATE TABLE imports (id INTEGER PRIMARY KEY, file_id INTEGER, module TEXT, alias TEXT);
    """)
    # Insert a dummy file so we can verify it gets cleared
    conn.execute("INSERT INTO files (path, hash, indexed_at) VALUES ('x.py', 'abc', 0.0)")
    conn.commit()
    conn.close()

    conn2 = init_db(db_path)
    cols = {c[1] for c in conn2.execute("PRAGMA table_info(symbols)").fetchall()}
    assert "language" in cols

    imp_cols = {c[1] for c in conn2.execute("PRAGMA table_info(imports)").fetchall()}
    assert "symbol" in imp_cols

    # files table cleared on migration
    count = conn2.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    assert count == 0


def test_migration_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / ".capsule" / "index.db"
    conn = init_db(db_path)
    conn.close()
    # Second call must not fail
    conn2 = init_db(db_path)
    cols = {c[1] for c in conn2.execute("PRAGMA table_info(symbols)").fetchall()}
    assert "language" in cols
