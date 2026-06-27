import os
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
CASES_DIR = REPO_ROOT / "integration-tests" / "cases"
CONFIG_PATH = REPO_ROOT / "integration-tests" / "dobbydb-integration.toml"
DOBBYDB_BIN = Path(os.environ.get("DOBBYDB_BIN", REPO_ROOT / "target" / "debug" / "dobbydb"))


@dataclass(frozen=True)
class QueryBlock:
    query: str
    expected: str


@dataclass(frozen=True)
class E2ETestFile:
    path: Path
    database: str
    blocks: list[QueryBlock]


def pytest_generate_tests(metafunc):
    if "test_file" in metafunc.fixturenames:
        files = sorted(CASES_DIR.glob("*.test"))
        metafunc.parametrize("test_file", files, ids=[str(path.relative_to(CASES_DIR)) for path in files])


def test_e2e_file(test_file: Path):
    parsed = parse_test_file(test_file)
    for index, block in enumerate(parsed.blocks, start=1):
        actual = run_query(block.query, parsed.database)
        expected = block.expected.strip()
        assert actual == expected, (
            f"{test_file}: query block {index} failed\n\n"
            f"SQL:\n{block.query.strip()}\n\n"
            f"Expected:\n{expected}\n\n"
            f"Actual:\n{actual}"
        )
    print(f"PASSED {test_file.relative_to(CASES_DIR)} ({len(parsed.blocks)} queries)", flush=True)


def parse_test_file(path: Path) -> E2ETestFile:
    metadata = {}
    blocks = []
    section = None
    query = []
    expected = []

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.rstrip()
        if section is None and line.startswith("-- ") and ":" in line:
            key, value = line[3:].split(":", 1)
            metadata[key.strip().lower()] = value.strip()
            continue

        if line == "-- QUERY":
            if section == "expected":
                blocks.append(build_block(path, query, expected))
                query = []
                expected = []
            section = "query"
        elif line == "-- EXPECTED":
            assert section == "query", f"{path}: -- EXPECTED must follow -- QUERY"
            section = "expected"
        elif section == "query":
            query.append(raw_line)
        elif section == "expected":
            expected.append(raw_line)
        elif line.strip():
            raise AssertionError(f"{path}: unexpected content before -- QUERY: {raw_line}")

    if section == "expected":
        blocks.append(build_block(path, query, expected))
    elif section is not None:
        raise AssertionError(f"{path}: unfinished query block")

    database = metadata.get("database")
    if not database:
        raise AssertionError(f"{path}: missing -- DATABASE metadata")
    if not blocks:
        raise AssertionError(f"{path}: no query blocks found")

    return E2ETestFile(path=path, database=database, blocks=blocks)


def build_block(path: Path, query: list[str], expected: list[str]) -> QueryBlock:
    query_text = "\n".join(query).strip()
    expected_text = "\n".join(expected).strip()
    if not query_text:
        raise AssertionError(f"{path}: empty -- QUERY block")
    if not expected_text:
        raise AssertionError(f"{path}: empty -- EXPECTED block")
    return QueryBlock(query=query_text, expected=expected_text)


def run_query(sql: str, database: str) -> str:
    with tempfile.NamedTemporaryFile("w", suffix=".sql", encoding="utf-8", delete=False) as query_file:
        query_file.write(sql)
        query_path = Path(query_file.name)

    try:
        output = subprocess.run(
            [
                str(DOBBYDB_BIN),
                "--config",
                str(CONFIG_PATH),
                "--default-catalog",
                "moto_glue",
                "--default-schema",
                database,
                "--file",
                str(query_path),
            ],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
    finally:
        query_path.unlink(missing_ok=True)

    if output.returncode != 0:
        raise AssertionError(f"DobbyDB query failed with status {output.returncode}\nSQL:\n{sql}\n\n{output.stdout}")

    return normalize_query_result(output.stdout)


def normalize_query_result(stdout: str) -> str:
    table_lines = [
        line.strip()
        for line in stdout.splitlines()
        if line.strip().startswith("|") and line.strip().endswith("|")
    ]
    rows = [
        [cell.strip() for cell in line.strip().strip("|").split("|")]
        for line in table_lines
    ]
    if not rows:
        return ""
    return "\n".join(",".join(row) for row in rows)
