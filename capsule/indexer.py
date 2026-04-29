import hashlib
import sqlite3
import time
from pathlib import Path
from typing import Optional

import tree_sitter_python as tspython
from tree_sitter import Language, Node, Parser

from capsule.models import File, Import, Symbol

PY_LANGUAGE = Language(tspython.language())
_parser = Parser(PY_LANGUAGE)


def get_project_root() -> Path:
    cwd = Path.cwd()
    for parent in [cwd, *cwd.parents]:
        if (parent / ".git").exists():
            return parent
    return cwd


def init_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=5)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY,
            path TEXT UNIQUE,
            hash TEXT,
            indexed_at REAL
        );
        CREATE TABLE IF NOT EXISTS symbols (
            id INTEGER PRIMARY KEY,
            file_id INTEGER,
            name TEXT,
            kind TEXT,
            start_line INTEGER,
            end_line INTEGER,
            signature TEXT,
            docstring TEXT,
            body TEXT,
            language TEXT DEFAULT 'python'
        );
        CREATE TABLE IF NOT EXISTS imports (
            id INTEGER PRIMARY KEY,
            file_id INTEGER,
            module TEXT,
            symbol TEXT,
            alias TEXT
        );
        CREATE VIRTUAL TABLE IF NOT EXISTS symbols_fts USING fts5(
            name, docstring, content='symbols', content_rowid='id'
        );
    """)

    migrated = False
    sym_cols = {c[1] for c in conn.execute("PRAGMA table_info(symbols)").fetchall()}
    imp_cols = {c[1] for c in conn.execute("PRAGMA table_info(imports)").fetchall()}

    with conn:
        if "language" not in sym_cols:
            conn.execute("ALTER TABLE symbols ADD COLUMN language TEXT DEFAULT 'python'")
            migrated = True
        if "symbol" not in imp_cols:
            conn.execute("ALTER TABLE imports ADD COLUMN symbol TEXT")
            migrated = True
        if migrated:
            conn.execute("DELETE FROM symbols")
            conn.execute("DELETE FROM imports")
            conn.execute("DELETE FROM files")

    return conn


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def is_stale(conn: sqlite3.Connection, path: Path) -> bool:
    row = conn.execute("SELECT hash FROM files WHERE path = ?", (str(path),)).fetchone()
    if row is None:
        return True
    return row[0] != _sha256(path)


def _node_text(source: bytes, node: Node) -> str:
    return source[node.start_byte:node.end_byte].decode("utf-8", errors="replace")


def _extract_docstring(source: bytes, body_node: Node) -> str:
    for child in body_node.named_children:
        if child.type == "expression_statement":
            for grandchild in child.named_children:
                if grandchild.type == "string":
                    raw = _node_text(source, grandchild)
                    return raw.strip("\"' \n").strip()
        break
    return ""


def _extract_py_symbols(source: bytes, root: Node, file_path: str) -> tuple[list[Symbol], list[Import]]:
    symbols: list[Symbol] = []
    imports: list[Import] = []

    def walk(node: Node, depth: int = 0) -> None:
        if node.type == "function_definition":
            name_node = node.child_by_field_name("name")
            params_node = node.child_by_field_name("parameters")
            body_node = node.child_by_field_name("body")
            if name_node:
                name = _node_text(source, name_node)
                params = _node_text(source, params_node) if params_node else "()"
                sig = f"def {name}{params}"
                doc = _extract_docstring(source, body_node) if body_node else ""
                body_text = _node_text(source, body_node) if body_node else ""
                symbols.append(Symbol(
                    file_path=file_path,
                    name=name,
                    kind="function",
                    start_line=node.start_point[0] + 1,
                    end_line=node.end_point[0] + 1,
                    signature=sig,
                    docstring=doc,
                    body=body_text,
                ))
            for child in node.named_children:
                walk(child, depth + 1)

        elif node.type == "class_definition":
            name_node = node.child_by_field_name("name")
            body_node = node.child_by_field_name("body")
            if name_node:
                name = _node_text(source, name_node)
                doc = _extract_docstring(source, body_node) if body_node else ""
                symbols.append(Symbol(
                    file_path=file_path,
                    name=name,
                    kind="class",
                    start_line=node.start_point[0] + 1,
                    end_line=node.end_point[0] + 1,
                    signature=f"class {name}",
                    docstring=doc,
                    body="",
                ))
            for child in node.named_children:
                walk(child, depth + 1)

        elif node.type == "import_statement":
            for child in node.named_children:
                if child.type == "dotted_name":
                    imports.append(Import(file_path=file_path, module=_node_text(source, child)))
                elif child.type == "aliased_import":
                    mod = child.child_by_field_name("name")
                    alias = child.child_by_field_name("alias")
                    if mod:
                        imports.append(Import(
                            file_path=file_path,
                            module=_node_text(source, mod),
                            alias=_node_text(source, alias) if alias else None,
                        ))

        elif node.type == "import_from_statement":
            mod_node = node.child_by_field_name("module_name")
            module = _node_text(source, mod_node) if mod_node else ""
            for child in node.named_children:
                if child.type == "dotted_name" and child != mod_node:
                    symbol_name = _node_text(source, child)
                    imports.append(Import(file_path=file_path, module=module, symbol=symbol_name))
                    break
                elif child.type == "aliased_import":
                    name_node = child.child_by_field_name("name")
                    alias_node = child.child_by_field_name("alias")
                    symbol_name = _node_text(source, name_node) if name_node else None
                    imports.append(Import(
                        file_path=file_path,
                        module=module,
                        symbol=symbol_name,
                        alias=_node_text(source, alias_node) if alias_node else None,
                    ))

        else:
            for child in node.named_children:
                walk(child, depth + 1)

    walk(root)
    return symbols, imports


def index_file(conn: sqlite3.Connection, path: Path) -> None:
    source = path.read_bytes()
    file_hash = hashlib.sha256(source).hexdigest()
    file_path = str(path)

    # Clean up FTS5 BEFORE deleting symbols (FTS5 references symbol rowids)
    old_file_row = conn.execute("SELECT id FROM files WHERE path = ?", (file_path,)).fetchone()
    if old_file_row:
        conn.execute(
            "DELETE FROM symbols_fts WHERE rowid IN (SELECT id FROM symbols WHERE file_id = ?)",
            (old_file_row[0],),
        )

    conn.execute("DELETE FROM symbols WHERE file_id = (SELECT id FROM files WHERE path = ?)", (file_path,))
    conn.execute("DELETE FROM imports WHERE file_id = (SELECT id FROM files WHERE path = ?)", (file_path,))
    conn.execute(
        "INSERT OR REPLACE INTO files (path, hash, indexed_at) VALUES (?, ?, ?)",
        (file_path, file_hash, time.time()),
    )
    file_id = conn.execute("SELECT id FROM files WHERE path = ?", (file_path,)).fetchone()[0]

    if path.suffix in (".ts", ".tsx"):
        from capsule.ts_parser import extract_ts_symbols, TS_AVAILABLE as _TS_AVAIL
        if not _TS_AVAIL:
            return
        symbols, imports = extract_ts_symbols(source, file_path, tsx=path.suffix == ".tsx")
        language = "typescript"
    else:
        tree = _parser.parse(source)
        symbols, imports = _extract_py_symbols(source, tree.root_node, file_path)
        language = "python"

    conn.executemany(
        "INSERT INTO symbols (file_id, name, kind, start_line, end_line, signature, docstring, body, language)"
        " VALUES (?,?,?,?,?,?,?,?,?)",
        [(file_id, s.name, s.kind, s.start_line, s.end_line, s.signature, s.docstring, s.body, language)
         for s in symbols],
    )
    conn.executemany(
        "INSERT INTO imports (file_id, module, symbol, alias) VALUES (?,?,?,?)",
        [(file_id, i.module, i.symbol, i.alias) for i in imports],
    )
    conn.execute(
        "INSERT INTO symbols_fts(rowid, name, docstring)"
        " SELECT id, name, COALESCE(docstring, '') FROM symbols WHERE file_id = ?",
        (file_id,),
    )
    conn.commit()


def index_project(root: Path) -> sqlite3.Connection:
    db_path = root / ".capsule" / "index.db"
    conn = init_db(db_path)
    for ext in ("*.py", "*.ts", "*.tsx"):
        for src_file in root.rglob(ext):
            if any(skip in src_file.parts for skip in (".capsule", ".worktrees")):
                continue
            if is_stale(conn, src_file):
                index_file(conn, src_file)
    return conn
