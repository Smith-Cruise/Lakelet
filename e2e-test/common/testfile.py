import difflib
import importlib.util
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class QueryBlock:
    query: str
    expected: str


@dataclass(frozen=True)
class E2ETestFile:
    path: Path
    name: str
    case: str
    database: str
    setup: str
    blocks: list[QueryBlock]


def load_test_file(path: Path) -> E2ETestFile:
    lines = path.read_text(encoding="utf-8").splitlines()
    metadata: dict[str, str] = {}
    blocks: list[QueryBlock] = []
    current_section: str | None = None
    current_query: list[str] = []
    current_expected: list[str] = []

    for raw_line in lines:
        line = raw_line.rstrip()
        if line.startswith("-- ") and ":" in line and current_section is None:
            key, value = line[3:].split(":", 1)
            metadata[key.strip().lower()] = value.strip()
            continue
        if line == "-- QUERY":
            if current_section == "expected":
                blocks.append(_build_block(current_query, current_expected, path))
                current_query = []
                current_expected = []
            current_section = "query"
            continue
        if line == "-- EXPECTED":
            if current_section != "query":
                raise ValueError(f"{path}: -- EXPECTED must follow -- QUERY")
            current_section = "expected"
            continue
        if current_section == "query":
            current_query.append(raw_line)
        elif current_section == "expected":
            current_expected.append(raw_line)
        elif line.strip():
            raise ValueError(f"{path}: unexpected content before -- QUERY: {raw_line}")

    if current_section == "expected":
        blocks.append(_build_block(current_query, current_expected, path))
    elif current_section is not None:
        raise ValueError(f"{path}: unfinished {current_section} block")

    required = ["test", "database", "setup"]
    missing = [key for key in required if key not in metadata]
    if missing:
        raise ValueError(f"{path}: missing metadata: {', '.join(missing)}")
    if not blocks:
        raise ValueError(f"{path}: no query blocks found")

    return E2ETestFile(
        path=path,
        name=metadata["test"],
        case=path.parent.name,
        database=metadata["database"],
        setup=metadata["setup"],
        blocks=blocks,
    )


def run_test_file(test_file: E2ETestFile, aws_context, dobbydb_runner) -> None:
    setup_module = _load_setup_module(test_file)
    setup_function_name = f"setup_{test_file.setup}"
    setup_function = getattr(setup_module, setup_function_name, None)
    if setup_function is None:
        raise AssertionError(
            f"{test_file.path}: setup function not found: {setup_function_name}"
        )
    setup_function(aws_context)

    for index, block in enumerate(test_file.blocks, start=1):
        result = dobbydb_runner.query(block.query)
        actual = normalize_query_result(result)
        expected = block.expected.strip()
        if actual != expected:
            diff = "\n".join(
                difflib.unified_diff(
                    expected.splitlines(),
                    actual.splitlines(),
                    fromfile="expected",
                    tofile="actual",
                    lineterm="",
                )
            )
            raise AssertionError(
                f"{test_file.path}: query block {index} failed\n\n"
                f"SQL:\n{block.query.strip()}\n\n"
                f"{diff}\n\nRaw output:\n{result.stdout}"
            )


def normalize_query_result(result) -> str:
    rows = result.rows
    lines = [",".join(result.header)]
    lines.extend(",".join(row) for row in rows)
    return "\n".join(lines)


def _build_block(
    query_lines: list[str], expected_lines: list[str], path: Path
) -> QueryBlock:
    query = "\n".join(query_lines).strip()
    expected = "\n".join(expected_lines).strip()
    if not query:
        raise ValueError(f"{path}: empty -- QUERY block")
    if not expected:
        raise ValueError(f"{path}: empty -- EXPECTED block")
    return QueryBlock(query=query, expected=expected)


def _load_setup_module(test_file: E2ETestFile):
    setup_path = test_file.path.parent / "setup.py"
    module_name = f"e2e_setup_{test_file.case}"
    spec = importlib.util.spec_from_file_location(module_name, setup_path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"Cannot load setup module: {setup_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
