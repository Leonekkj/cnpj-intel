import warnings

from capsule.models import Import, Symbol

try:
    import tree_sitter_typescript as tsts
    from tree_sitter import Language, Parser as TSParser

    _TS_LANGUAGE = Language(tsts.language_typescript())
    _TSX_LANGUAGE = Language(tsts.language_tsx())
    _TS_PARSER = TSParser(_TS_LANGUAGE)
    _TSX_PARSER = TSParser(_TSX_LANGUAGE)
    TS_AVAILABLE = True
except (ImportError, AttributeError, OSError):
    TS_AVAILABLE = False
    warnings.warn("[capsule] tree-sitter-typescript not installed — TS/TSX files skipped")


def _node_text(source: bytes, node) -> str:
    return source[node.start_byte:node.end_byte].decode("utf-8", errors="replace")


def extract_ts_symbols(source: bytes, file_path: str, tsx: bool = False) -> tuple[list[Symbol], list[Import]]:
    if not TS_AVAILABLE:
        return [], []
    parser = _TSX_PARSER if tsx else _TS_PARSER
    tree = parser.parse(source)
    return _walk(source, tree.root_node, file_path)


def _walk(source: bytes, root, file_path: str) -> tuple[list[Symbol], list[Import]]:
    symbols: list[Symbol] = []
    imports: list[Import] = []

    def walk(node) -> None:
        if node.type == "function_declaration":
            name_node = node.child_by_field_name("name")
            params_node = node.child_by_field_name("parameters")
            if name_node:
                name = _node_text(source, name_node)
                params = _node_text(source, params_node) if params_node else "()"
                symbols.append(Symbol(
                    file_path=file_path,
                    name=name,
                    kind="function",
                    start_line=node.start_point[0] + 1,
                    end_line=node.end_point[0] + 1,
                    signature=f"function {name}{params}",
                ))

        elif node.type == "class_declaration":
            name_node = node.child_by_field_name("name")
            if name_node:
                name = _node_text(source, name_node)
                symbols.append(Symbol(
                    file_path=file_path,
                    name=name,
                    kind="class",
                    start_line=node.start_point[0] + 1,
                    end_line=node.end_point[0] + 1,
                    signature=f"class {name}",
                ))

        elif node.type == "interface_declaration":
            name_node = node.child_by_field_name("name")
            if name_node:
                name = _node_text(source, name_node)
                symbols.append(Symbol(
                    file_path=file_path,
                    name=name,
                    kind="interface",
                    start_line=node.start_point[0] + 1,
                    end_line=node.end_point[0] + 1,
                    signature=f"interface {name}",
                ))

        elif node.type == "type_alias_declaration":
            name_node = node.child_by_field_name("name")
            if name_node:
                name = _node_text(source, name_node)
                symbols.append(Symbol(
                    file_path=file_path,
                    name=name,
                    kind="type",
                    start_line=node.start_point[0] + 1,
                    end_line=node.end_point[0] + 1,
                    signature=f"type {name}",
                ))

        elif node.type == "import_statement":
            source_node = node.child_by_field_name("source")
            if source_node:
                module = _node_text(source, source_node).strip("'\"")
                for child in node.named_children:
                    if child.type == "import_clause":
                        for grandchild in child.named_children:
                            if grandchild.type == "named_imports":
                                for spec in grandchild.named_children:
                                    if spec.type == "import_specifier":
                                        name_node = spec.child_by_field_name("name")
                                        alias_node = spec.child_by_field_name("alias")
                                        if name_node:
                                            imports.append(Import(
                                                file_path=file_path,
                                                module=module,
                                                symbol=_node_text(source, name_node),
                                                alias=_node_text(source, alias_node) if alias_node else None,
                                            ))

        for child in node.named_children:
            walk(child)

    walk(root)
    return symbols, imports
