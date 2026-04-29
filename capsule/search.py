import sqlite3

from capsule.models import Symbol


def _row_to_symbol(row: tuple) -> Symbol:
    return Symbol(
        file_path=row[0],
        name=row[1],
        kind=row[2],
        start_line=row[3],
        end_line=row[4],
        signature=row[5],
        docstring=row[6],
        body=row[7],
    )


def search_symbols(conn: sqlite3.Connection, query: str, max_results: int = 20) -> list[Symbol]:
    # FTS5 first — faster and rank-ordered
    try:
        rows = conn.execute(
            """
            SELECT f.path, s.name, s.kind, s.start_line, s.end_line, s.signature, s.docstring, s.body
            FROM symbols s
            JOIN files f ON s.file_id = f.id
            JOIN symbols_fts fts ON s.id = fts.rowid
            WHERE symbols_fts MATCH ?
            ORDER BY rank
            LIMIT ?
            """,
            (query, max_results),
        ).fetchall()
        if rows:
            return [_row_to_symbol(r) for r in rows]
    except sqlite3.OperationalError:
        pass

    # LIKE fallback
    pattern = f"%{query}%"
    rows = conn.execute(
        """
        SELECT f.path, s.name, s.kind, s.start_line, s.end_line, s.signature, s.docstring, s.body
        FROM symbols s
        JOIN files f ON s.file_id = f.id
        WHERE s.name LIKE ? OR s.docstring LIKE ?
        ORDER BY
            CASE WHEN s.name LIKE ? THEN 0 ELSE 1 END,
            s.name
        LIMIT ?
        """,
        (pattern, pattern, pattern, max_results),
    ).fetchall()
    return [_row_to_symbol(r) for r in rows]
