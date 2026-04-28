# Capsule Phase 2 Design

**Date:** 2026-04-28
**Status:** Approved

## Context

Phase 1 of Capsule is complete and tested (12/12 passing). It indexes Python files with tree-sitter, stores symbols in SQLite, and exposes two MCP tools (`get_skeleton`, `get_context_capsule`) over stdio. Phase 2 extends the system with TypeScript support, FTS5 search, symbol-level import tracking, and an impact graph tool.

## File Structure

```
capsule/
  models.py      — Add symbol: Optional[str] to Import dataclass
  indexer.py     — Schema migrations, FTS5 population, TS dispatch, Python extraction
  ts_parser.py   — NEW: conditional tree-sitter-typescript, TS symbol/import extraction
  search.py      — FTS5-first search with LIKE fallback
  server.py      — Add get_impact_graph tool
tests/
  test_indexer.py   — existing, must stay green
  test_search.py    — existing, must stay green
  test_phase2.py    — NEW: 4 tests for Phase 2 features
```

Each file stays ≤ 300 lines. TS extraction is isolated in `ts_parser.py` to keep `indexer.py` focused and within the line limit.

## Schema Changes

All migrations run inside `init_db()`. After any column addition, the `files` table is cleared to force a full re-index — ensures no NULL gaps in impact graph data.

```sql
-- Add language column to symbols (default 'python')
-- Check via PRAGMA table_info(symbols) before ALTER TABLE

ALTER TABLE symbols ADD COLUMN language TEXT DEFAULT 'python'

-- Add symbol column to imports (tracks imported name, not just module)
-- Check via PRAGMA table_info(imports) before ALTER TABLE

ALTER TABLE imports ADD COLUMN symbol TEXT

-- FTS5 virtual table mirroring symbols(name, docstring)
CREATE VIRTUAL TABLE IF NOT EXISTS symbols_fts USING fts5(
    name, docstring, content='symbols', content_rowid='id'
)
```

Migration pattern (SQLite doesn't support `ALTER TABLE ADD COLUMN IF NOT EXISTS`):
```python
cols = {c[1] for c in conn.execute("PRAGMA table_info(symbols)").fetchall()}
if "language" not in cols:
    conn.execute("ALTER TABLE symbols ADD COLUMN language TEXT DEFAULT 'python'")
    conn.execute("DELETE FROM files")  # force full re-index
    conn.commit()
```

## TypeScript Support (`ts_parser.py`)

Conditional import — no hard dependency:
```python
try:
    import tree_sitter_typescript as tsts
    _TS_AVAILABLE = True
except ImportError:
    _TS_AVAILABLE = False
    warnings.warn("[capsule] tree-sitter-typescript not installed — TS/TSX files skipped")
```

Extracts from TS AST:
- `function_declaration` → kind="function"
- `class_declaration` → kind="class"
- `interface_declaration` → kind="interface"
- `type_alias_declaration` → kind="type"
- `import_statement` / `import_from_statement` → `Import(module, symbol, alias)`

`indexer.py` dispatches by extension:
```python
def index_file(conn, path: Path) -> None:
    if path.suffix in (".ts", ".tsx"):
        symbols, imports = extract_ts_symbols(source, tree.root_node, file_path)
        language = "typescript"
    else:
        symbols, imports = _extract_py_symbols(source, tree.root_node, file_path)
        language = "python"
```

`index_project()` globs both:
```python
for ext in ("*.py", "*.ts", "*.tsx"):
    for f in root.rglob(ext):
        if any(skip in f.parts for skip in (".capsule", ".worktrees")):
            continue
        if is_stale(conn, f):
            index_file(conn, f)
```

## FTS5 Integration

**Creation** (`init_db`): `CREATE VIRTUAL TABLE IF NOT EXISTS symbols_fts USING fts5(...)`

**Population** (`index_file`): after inserting symbols rows, sync FTS5:
```python
conn.execute(
    "DELETE FROM symbols_fts WHERE rowid IN (SELECT id FROM symbols WHERE file_id = ?)",
    (file_id,)
)
conn.execute(
    "INSERT INTO symbols_fts(rowid, name, docstring) "
    "SELECT id, name, COALESCE(docstring, '') FROM symbols WHERE file_id = ?",
    (file_id,)
)
```

**Query** (`search_symbols`): FTS5 first, LIKE fallback:
```python
try:
    rows = conn.execute(
        "SELECT s.* FROM symbols s JOIN symbols_fts f ON s.id = f.rowid "
        "WHERE symbols_fts MATCH ? ORDER BY rank LIMIT ?",
        (query, max_results)
    ).fetchall()
    if rows:
        return [Symbol(...) for r in rows]
except sqlite3.OperationalError:
    pass
# fall through to LIKE query
```

## Import Tracking Extension

`Import` dataclass:
```python
@dataclass
class Import:
    file_path: str
    module: str
    symbol: Optional[str] = None   # NEW: name of imported symbol
    alias: Optional[str] = None
```

`imports` table gains `symbol TEXT` column (migration as above).

Extractor change in `import_from_statement` branch — for `from api import autenticar_token`:
- Before: `Import(module='api', alias=None)`
- After: `Import(module='api', symbol='autenticar_token', alias=None)`

For `from api import autenticar_token as auth`:
- After: `Import(module='api', symbol='autenticar_token', alias='auth')`

Bare `import os` stays: `Import(module='os', symbol=None, alias=None)`.

## `get_impact_graph` Tool

Added to `server.py`:

```python
@app.tool()
def get_impact_graph(symbol: str, depth: int = 2) -> str:
    """
    Returns files/symbols affected if `symbol` changes.
    depth is reserved for Phase 3 transitive resolution; Phase 2 resolves 1 level.
    """
```

Logic:
1. Query `symbols` table for the file that defines `symbol`
2. Query `imports` table for all files with `symbol = <name>` (exact match)
3. Compute `risk_level`: 0 importers → "low", 1–2 → "medium", 3+ → "high"
4. Return YAML

Output format:
```yaml
symbol: autenticar_token
defined_in: api.py
imported_by:
  - agent/agent.py
  - tests/test_api.py
risk_level: medium
note: depth > 1 not yet implemented
```

If symbol is not found in index, return YAML with an `error` key.

## Tests (`test_phase2.py`)

| Test | Fixture | Assertion |
|------|---------|-----------|
| `test_typescript_indexing` | Synthetic `.ts` file with a function + interface | Both appear in `symbols` with `language='typescript'` |
| `test_fts5_search` | Populated DB with known symbol | `search_symbols` returns that symbol (verifies FTS5 path taken) |
| `test_impact_graph_no_dependents` | Symbol defined in one file, no importers | YAML has `risk_level: low`, `imported_by: []` |
| `test_impact_graph_with_dependents` | Symbol imported by 3 synthetic files | YAML has `risk_level: high` |

All 4 tests use `tmp_path` fixtures. Existing 12 tests must remain unmodified and passing.

## Constraints

- All files ≤ 300 lines
- `pathlib.Path` everywhere, no string path concatenation
- Existing 12 tests unmodified and passing
- TS support degrades gracefully if `tree-sitter-typescript` is absent
- `depth > 1` in `get_impact_graph` is reserved — Phase 2 always resolves 1 level

## Verification

```bash
pip install tree-sitter-typescript
cd C:/Users/ideia/OneDrive/Desktop/CNPJ/.worktrees/capsule-mcp
python -m pytest tests/ -v          # all 16 tests green
python -m capsule.server            # starts without error, prints index ready
# In Claude Code after restart: call get_impact_graph("autenticar_token")
# → YAML with defined_in, imported_by, risk_level
```
