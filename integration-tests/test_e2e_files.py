import datetime
import os
import re
import socket
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import adbc_driver_flightsql.dbapi as flight_sql
import pytest
from adbc_driver_flightsql import DatabaseOptions


REPO_ROOT = Path(__file__).resolve().parents[1]
CASES_DIR = REPO_ROOT / "integration-tests" / "cases"
CONFIG_PATH = REPO_ROOT / "integration-tests" / "lakelet-integration.toml"
LAKELET_BIN = Path(os.environ.get("LAKELET_BIN", REPO_ROOT / "target" / "debug" / "lakelet"))

# lakelet-integration.toml has no [server] table, so the server listens on the
# default flight-sql-server-port.
FLIGHT_PORT = 32010
SERVER_READY_TIMEOUT_SECONDS = 30
CALL_HEADER = DatabaseOptions.RPC_CALL_HEADER_PREFIX.value


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


@pytest.fixture(scope="session")
def flight_server():
    """One lakelet --flight-sql-server process shared by every test."""
    process = subprocess.Popen(
        [str(LAKELET_BIN), "--config", str(CONFIG_PATH), "--flight-sql-server"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        wait_for_server(process)
        yield f"grpc://127.0.0.1:{FLIGHT_PORT}"
    finally:
        process.terminate()
        process.wait()


def wait_for_server(process: subprocess.Popen) -> None:
    deadline = time.monotonic() + SERVER_READY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError(
                f"Lakelet Flight SQL server exited with status {process.returncode}\n\n{process.stdout.read()}"
            )
        try:
            with socket.create_connection(("127.0.0.1", FLIGHT_PORT), timeout=1):
                return
        except OSError:
            time.sleep(0.1)
    process.terminate()
    raise AssertionError(
        f"Lakelet Flight SQL server did not open port {FLIGHT_PORT} within {SERVER_READY_TIMEOUT_SECONDS}s"
    )


def connect(uri: str, database: str):
    # The default-catalog/default-schema gRPC headers replace the CLI
    # --default-catalog/--default-schema flags per connection; set as
    # connection-level call headers they ride along on every RPC.
    return flight_sql.connect(
        uri,
        db_kwargs={
            CALL_HEADER + "default-catalog": "moto_glue",
            CALL_HEADER + "default-schema": database,
        },
    )


def test_e2e_file(test_file: Path, flight_server: str):
    parsed = parse_test_file(test_file)
    with connect(flight_server, parsed.database) as conn:
        for index, block in enumerate(parsed.blocks, start=1):
            actual = run_query(conn, block.query)
            expected = block.expected.strip()
            assert assert_result_matches(actual, expected), (
                f"{test_file}: query block {index} failed\n\n"
                f"SQL:\n{block.query.strip()}\n\n"
                f"Expected:\n{expected}\n\n"
                f"Actual:\n{actual}"
            )
    print(f"PASSED {test_file.relative_to(CASES_DIR)} ({len(parsed.blocks)} queries)", flush=True)


def assert_result_matches(actual: str, expected: str) -> bool:
    if "{{ANY}}" not in expected:
        return actual == expected

    pattern = re.escape(expected).replace(re.escape("{{ANY}}"), ".*")
    return re.fullmatch(pattern, actual, flags=re.DOTALL) is not None


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


def run_query(conn, sql: str) -> str:
    with conn.cursor() as cursor:
        try:
            cursor.execute(sql)
            table = cursor.fetch_arrow_table()
        except Exception as e:
            raise AssertionError(f"Lakelet query failed\nSQL:\n{sql}\n\n{e}") from e
    return format_arrow_table(table)


def format_arrow_table(table) -> str:
    """Render an Arrow table the way the old REPL output normalized: a header
    line of schema field names, then one comma-joined line per row."""
    lines = [",".join(table.schema.names)]
    columns = table.columns
    for row_index in range(table.num_rows):
        cells = [format_cell(column[row_index].as_py()) for column in columns]
        lines.append(",".join(cells))
    # Multi-line cells (e.g. SHOW CREATE TABLE) expand into physical lines;
    # strip each one, matching how the old table output was normalized.
    return "\n".join(line.strip() for text in lines for line in text.split("\n"))


def format_cell(value) -> str:
    if value is None:
        # NULL rendered as an empty cell in the old table output.
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime.datetime):
        # Arrow's display uses the RFC 3339 "T" separator.
        return value.isoformat()
    # str() keeps Decimal scale ("19.30") and matches dates ("2026-06-24").
    return str(value)
