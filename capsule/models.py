from dataclasses import dataclass, field
from typing import Optional


@dataclass
class File:
    path: str
    hash: str
    indexed_at: float


@dataclass
class Symbol:
    file_path: str
    name: str
    kind: str  # "function" | "class"
    start_line: int
    end_line: int
    signature: str = ""
    docstring: str = ""
    body: str = ""


@dataclass
class Import:
    file_path: str
    module: str
    alias: Optional[str] = None
