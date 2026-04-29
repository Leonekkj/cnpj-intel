import sqlite3
from pathlib import Path
from typing import Literal

import yaml
from mcp.server.fastmcp import FastMCP

from capsule.indexer import get_project_root, index_project
from capsule.models import Symbol
from capsule.search import search_symbols

app = FastMCP("capsule")

_conn: sqlite3.Connection | None = None


def _get_conn() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        root = get_project_root()
        _conn = index_project(root)
    return _conn


def _symbol_to_dict(sym: Symbol, detail: str) -> dict:
    entry: dict = {
        "name": sym.name,
        "kind": sym.kind,
        "file": sym.file_path,
        "lines": f"{sym.start_line}-{sym.end_line}",
    }
    if detail in ("standard", "detailed"):
        entry["signature"] = sym.signature
        if sym.docstring:
            entry["docstring"] = sym.docstring
    if detail == "detailed" and sym.body and (sym.end_line - sym.start_line) <= 15:
        entry["body"] = sym.body
    return entry


@app.tool()
def get_skeleton(path: str, detail: Literal["minimal", "standard", "detailed"] = "standard") -> str:
    """Return YAML skeleton of all symbols in a Python file."""
    conn = _get_conn()
    abs_path = str(Path(get_project_root() / path).resolve())
    rows = conn.execute(
        """
        SELECT s.name, s.kind, s.start_line, s.end_line, s.signature, s.docstring, s.body
        FROM symbols s JOIN files f ON s.file_id = f.id
        WHERE f.path = ?
        ORDER BY s.start_line
        """,
        (abs_path,),
    ).fetchall()

    if not rows:
        return yaml.dump({"error": f"No symbols found for {path}. Try re-indexing."})

    symbols = [
        Symbol(
            file_path=abs_path,
            name=r[0], kind=r[1],
            start_line=r[2], end_line=r[3],
            signature=r[4], docstring=r[5], body=r[6],
        )
        for r in rows
    ]
    data = {"file": path, "symbols": [_symbol_to_dict(s, detail) for s in symbols]}
    return yaml.dump(data, allow_unicode=True, default_flow_style=False)


@app.tool()
def get_context_capsule(task: str, max_symbols: int = 20) -> str:
    """Search symbols relevant to a task description and return YAML context."""
    conn = _get_conn()
    symbols = search_symbols(conn, task, max_results=max_symbols)
    if not symbols:
        return yaml.dump({"task": task, "symbols": [], "message": "No matching symbols found."})
    data = {
        "task": task,
        "symbols": [_symbol_to_dict(s, "standard") for s in symbols],
    }
    return yaml.dump(data, allow_unicode=True, default_flow_style=False)


@app.tool()
def get_impact_graph(symbol: str, depth: int = 2) -> str:
    """Return files affected if symbol changes, using symbol-level import tracking."""
    conn = _get_conn()

    defined_row = conn.execute(
        "SELECT f.path FROM symbols s JOIN files f ON s.file_id = f.id WHERE s.name = ? LIMIT 1",
        (symbol,),
    ).fetchone()

    if not defined_row:
        return yaml.dump({"error": f"Symbol '{symbol}' not found in index."})

    defined_in = defined_row[0]

    import_rows = conn.execute(
        "SELECT DISTINCT f.path FROM imports i JOIN files f ON i.file_id = f.id WHERE i.symbol = ?",
        (symbol,),
    ).fetchall()
    imported_by = [r[0] for r in import_rows]
    count = len(imported_by)

    risk_level = "low" if count == 0 else ("medium" if count <= 2 else "high")

    data: dict = {
        "symbol": symbol,
        "defined_in": defined_in,
        "imported_by": imported_by,
        "risk_level": risk_level,
    }
    if depth > 1:
        data["note"] = "depth > 1 not yet implemented"

    return yaml.dump(data, allow_unicode=True, default_flow_style=False)


if __name__ == "__main__":
    root = get_project_root()
    print(f"[capsule] Indexing {root} ...")
    _conn = index_project(root)
    print("[capsule] Index ready. Starting MCP server (stdio).")
    app.run(transport="stdio")
