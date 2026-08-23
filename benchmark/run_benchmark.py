#!/usr/bin/env python3
import argparse
import csv
import os
import socket
import subprocess
import sys
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Sequence


RUNS_PER_QUERY = 1
SUPPORTED_BENCHMARK_TYPES = ("tpch",)
FLIGHT_SQL_HOST = "127.0.0.1"
DEFAULT_FLIGHT_SQL_SERVER_PORT = 32010
SERVER_READY_TIMEOUT_SECONDS = 30
SERVER_SHUTDOWN_TIMEOUT_SECONDS = 10


class BenchmarkError(Exception):
    def __init__(self, message: str, raw_output: str | None = None) -> None:
        super().__init__(message)
        self.raw_output = raw_output


class BenchmarkInfrastructureError(BenchmarkError):
    pass


@dataclass(frozen=True)
class QueryRunResult:
    elapsed_seconds: float
    raw_output: str


class BenchmarkOutput:
    def __init__(self, run_dir: Path) -> None:
        self.run_dir = run_dir
        self.raw_dir = run_dir / "raw"
        self.console_path = run_dir / "console.txt"
        self.results_path = run_dir / "results.csv"

        self.raw_dir.mkdir(parents=True, exist_ok=False)
        self.console_file = self.console_path.open("w", encoding="utf-8")
        self.results_file = self.results_path.open("w", newline="", encoding="utf-8")
        self.results_writer = csv.writer(self.results_file)
        self.results_writer.writerow(["query", "run", "status", "elapsed_seconds", "error"])
        self.results_file.flush()

    @classmethod
    def create(cls, output_root: Path) -> "BenchmarkOutput":
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        output_root = output_root.expanduser()
        if not output_root.is_absolute():
            output_root = output_root.resolve()
        run_dir = output_root / f"lakelet-benchmark-{timestamp}-{os.getpid()}"
        run_dir.mkdir(parents=True, exist_ok=False)
        return cls(run_dir)

    def log(self, message: str = "") -> None:
        print(message)
        print(message, file=self.console_file)
        self.console_file.flush()

    def write_result(self, query_name: str, run_index: int, result: QueryRunResult) -> None:
        self.results_writer.writerow(
            [query_name, run_index, "success", format_seconds(result.elapsed_seconds), ""]
        )
        self.results_file.flush()
        self.write_raw_output(query_name, run_index, result.raw_output)

    def write_failure(
        self,
        query_name: str,
        run_index: int,
        error: BenchmarkError,
    ) -> None:
        error_message = " ".join(str(error).splitlines())
        self.results_writer.writerow(
            [query_name, run_index, "failed", "", error_message]
        )
        self.results_file.flush()
        if error.raw_output is not None:
            self.write_raw_output(query_name, run_index, error.raw_output)

    def write_raw_output(self, query_name: str, run_index: int, raw_output: str) -> None:
        query_dir = self.raw_dir / query_name
        query_dir.mkdir(parents=True, exist_ok=True)
        raw_path = query_dir / f"run{run_index}.txt"
        raw_path.write_text(raw_output, encoding="utf-8")

    def close(self) -> None:
        self.results_file.close()
        self.console_file.close()


class BenchmarkSuite:
    def __init__(
        self, repo_root: Path, benchmark_type: str, query_names: Sequence[str] | None
    ) -> None:
        if benchmark_type not in SUPPORTED_BENCHMARK_TYPES:
            supported = ", ".join(SUPPORTED_BENCHMARK_TYPES)
            raise BenchmarkError(
                f"unsupported benchmark type: {benchmark_type}. Supported values: {supported}"
            )

        self.benchmark_type = benchmark_type
        self.sql_dir = repo_root / "benchmark" / benchmark_type
        if not self.sql_dir.is_dir():
            raise BenchmarkError(f"benchmark SQL directory does not exist: {self.sql_dir}")

        self.sql_files = sorted(self.sql_dir.glob("q*.sql"))
        if not self.sql_files:
            raise BenchmarkError(f"no SQL files found under {self.sql_dir}")

        if query_names:
            requested = {normalize_query_name(query_name) for query_name in query_names}
            sql_files_by_name = {sql_file.stem: sql_file for sql_file in self.sql_files}
            missing = sorted(requested.difference(sql_files_by_name))
            if missing:
                raise BenchmarkError(
                    f"benchmark SQL files do not exist: {', '.join(missing)}"
                )
            self.sql_files = [sql_files_by_name[query_name] for query_name in sorted(requested)]


class EngineRunner(ABC):
    def prepare(self) -> None:
        pass

    @abstractmethod
    def run_query(self, sql_file: Path) -> QueryRunResult:
        pass

    def close(self) -> None:
        pass


class LakeletRunner(EngineRunner):
    def __init__(
        self,
        repo_root: Path,
        bin_path: Path,
        config_path: Path,
        default_catalog: str,
        default_schema: str,
        server_log_path: Path,
    ) -> None:
        self.repo_root = repo_root
        self.bin_path = bin_path
        self.config_path = config_path
        self.default_catalog = default_catalog
        self.default_schema = default_schema
        self.server_log_path = server_log_path
        self.server_port = None
        self.server_process = None
        self.server_log_file = None
        self.connection = None

        if not self.bin_path.is_file() or not os.access(self.bin_path, os.X_OK):
            raise BenchmarkError(f"binary is not executable: {self.bin_path}")
        if not self.config_path.is_file():
            raise BenchmarkError(f"config file does not exist: {self.config_path}")

    def prepare(self) -> None:
        flight_sql, database_options = load_adbc_driver()
        self.server_port = read_flight_sql_server_port(self.config_path)
        ensure_port_available(FLIGHT_SQL_HOST, self.server_port)
        command = [
            str(self.bin_path),
            "--config",
            str(self.config_path),
            "--flight-sql-server",
        ]

        try:
            self.server_log_file = self.server_log_path.open("w", encoding="utf-8")
            self.server_process = subprocess.Popen(
                command,
                cwd=self.repo_root,
                text=True,
                stdout=self.server_log_file,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            wait_for_server(
                self.server_process,
                FLIGHT_SQL_HOST,
                self.server_port,
                SERVER_READY_TIMEOUT_SECONDS,
            )
            header_prefix = database_options.RPC_CALL_HEADER_PREFIX.value
            self.connection = flight_sql.connect(
                f"grpc://{FLIGHT_SQL_HOST}:{self.server_port}",
                db_kwargs={
                    header_prefix + "default-catalog": self.default_catalog,
                    header_prefix + "default-schema": self.default_schema,
                },
            )
        except Exception as exc:
            self.close()
            details = read_server_log(self.server_log_path)
            message = f"failed to start Lakelet Flight SQL server: {exc}"
            if details:
                message += f"\n\nLakelet server log:\n{details}"
            raise BenchmarkInfrastructureError(message) from exc

    def run_query(self, sql_file: Path) -> QueryRunResult:
        if self.connection is None or self.server_process is None:
            raise BenchmarkInfrastructureError(
                "Lakelet ADBC connection has not been prepared"
            )
        self._ensure_server_running()

        sql = sql_file.read_text(encoding="utf-8")
        table = None
        start = time.perf_counter()
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
                table = cursor.fetch_arrow_table()
        except Exception as exc:
            self._ensure_server_running()
            raw_output = format_lakelet_output(
                query_name=sql_file.stem,
                sql=sql,
                status="failed",
                elapsed_seconds=None,
                table=None,
                error=str(exc),
            )
            raise BenchmarkError(
                f"Lakelet query failed: {exc}", raw_output=raw_output
            ) from exc
        elapsed_seconds = time.perf_counter() - start
        raw_output = format_lakelet_output(
            query_name=sql_file.stem,
            sql=sql,
            status="success",
            elapsed_seconds=elapsed_seconds,
            table=table,
            error=None,
        )
        return QueryRunResult(elapsed_seconds=elapsed_seconds, raw_output=raw_output)

    def _ensure_server_running(self) -> None:
        if self.server_process is None:
            raise BenchmarkInfrastructureError(
                "Lakelet Flight SQL server was not started"
            )
        returncode = self.server_process.poll()
        if returncode is None:
            return
        details = read_server_log(self.server_log_path)
        message = f"Lakelet Flight SQL server exited with status {returncode}"
        if details:
            message += f"\n\nLakelet server log:\n{details}"
        raise BenchmarkInfrastructureError(message)

    def close(self) -> None:
        try:
            if self.connection is not None:
                self.connection.close()
        finally:
            self.connection = None
            try:
                stop_process(self.server_process, SERVER_SHUTDOWN_TIMEOUT_SECONDS)
            finally:
                self.server_process = None
                if self.server_log_file is not None:
                    self.server_log_file.close()
                    self.server_log_file = None


class StarRocksRunner(EngineRunner):
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str | None,
        default_catalog: str,
        default_schema: str,
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.default_catalog = default_catalog
        self.default_schema = default_schema
        self.connection = None

    def prepare(self) -> None:
        try:
            import pymysql
        except ImportError as exc:
            raise BenchmarkError(
                "PyMySQL is required for StarRocks benchmarks. "
                "Install it with: pip install -r benchmark/requirements.txt"
            ) from exc

        try:
            self.connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password or "",
                autocommit=True,
            )
            with self.connection.cursor() as cursor:
                catalog = quote_identifier(self.default_catalog)
                schema = quote_identifier(self.default_schema)
                cursor.execute(f"USE {catalog}.{schema}")
        except Exception as exc:
            self.close()
            raise BenchmarkError(f"failed to connect to StarRocks: {exc}") from exc

    def run_query(self, sql_file: Path) -> QueryRunResult:
        if self.connection is None:
            raise BenchmarkError("StarRocks connection has not been prepared")

        sql = sql_file.read_text(encoding="utf-8")
        result_sets = []
        start = time.perf_counter()
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
                while True:
                    columns = (
                        [description[0] for description in cursor.description]
                        if cursor.description
                        else []
                    )
                    rows = cursor.fetchall()
                    result_sets.append((columns, rows))
                    if not cursor.nextset():
                        break
        except Exception as exc:
            raw_output = format_starrocks_output(
                query_name=sql_file.stem,
                sql=sql,
                status="failed",
                elapsed_seconds=None,
                result_sets=result_sets,
                error=str(exc),
            )
            raise BenchmarkError(
                f"StarRocks query failed: {exc}", raw_output=raw_output
            ) from exc

        elapsed_seconds = time.perf_counter() - start
        raw_output = format_starrocks_output(
            query_name=sql_file.stem,
            sql=sql,
            status="success",
            elapsed_seconds=elapsed_seconds,
            result_sets=result_sets,
            error=None,
        )
        return QueryRunResult(elapsed_seconds=elapsed_seconds, raw_output=raw_output)

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None


def quote_identifier(identifier: str) -> str:
    escaped = identifier.replace("`", "``")
    return f"`{escaped}`"


def format_lakelet_output(
    query_name: str,
    sql: str,
    status: str,
    elapsed_seconds: float | None,
    table: object | None,
    error: str | None,
) -> str:
    lines = [
        f"query={query_name}",
        "protocol=adbc-flight-sql",
        f"status={status}",
    ]
    if elapsed_seconds is not None:
        lines.append(f"elapsed_seconds={format_seconds(elapsed_seconds)}")
    if error is not None:
        lines.append(f"error={error}")

    lines.extend(["", "sql:", sql.rstrip(), "", "output:"])
    if table is None:
        lines.append("(no result captured)")
    else:
        columns, rows = arrow_table_to_result_set(table)
        lines.append(format_result_set(columns, rows))
    lines.append("")
    return "\n".join(lines)


def arrow_table_to_result_set(
    table: object,
) -> tuple[Sequence[str], Sequence[Sequence[object]]]:
    columns = list(table.schema.names)
    rows = [
        [column[row_index].as_py() for column in table.columns]
        for row_index in range(table.num_rows)
    ]
    return columns, rows


def load_adbc_driver():
    try:
        import adbc_driver_flightsql.dbapi as flight_sql
        from adbc_driver_flightsql import DatabaseOptions
    except ImportError as exc:
        raise BenchmarkInfrastructureError(
            "adbc-driver-flightsql and pyarrow are required for Lakelet benchmarks. "
            "Install them with: pip install -r benchmark/requirements.txt"
        ) from exc
    return flight_sql, DatabaseOptions


def read_flight_sql_server_port(config_path: Path) -> int:
    try:
        try:
            import tomllib
        except ModuleNotFoundError:
            import tomli as tomllib
    except ImportError as exc:
        raise BenchmarkInfrastructureError(
            "tomli is required to read Lakelet config files on Python 3.10. "
            "Install it with: pip install -r benchmark/requirements.txt"
        ) from exc

    try:
        with config_path.open("rb") as config_file:
            config = tomllib.load(config_file)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise BenchmarkInfrastructureError(
            f"failed to read Lakelet config {config_path}: {exc}"
        ) from exc

    server = config.get("server", {})
    if not isinstance(server, dict):
        raise BenchmarkInfrastructureError(
            f"invalid [server] table in Lakelet config: {config_path}"
        )
    port = server.get("flight-sql-server-port", DEFAULT_FLIGHT_SQL_SERVER_PORT)
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise BenchmarkInfrastructureError(
            "flight-sql-server-port must be an integer between 1 and 65535"
        )
    return port


def ensure_port_available(host: str, port: int) -> None:
    try:
        with socket.create_connection((host, port), timeout=0.2):
            pass
    except OSError:
        return
    raise BenchmarkInfrastructureError(
        f"Flight SQL server port {host}:{port} is already in use"
    )


def wait_for_server(
    process: subprocess.Popen,
    host: str,
    port: int,
    timeout_seconds: float,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        returncode = process.poll()
        if returncode is not None:
            raise BenchmarkInfrastructureError(
                f"Lakelet Flight SQL server exited with status {returncode}"
            )
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except OSError:
            time.sleep(0.1)
    raise BenchmarkInfrastructureError(
        f"Lakelet Flight SQL server did not open {host}:{port} "
        f"within {timeout_seconds:g} seconds"
    )


def stop_process(process: subprocess.Popen | None, timeout_seconds: float) -> None:
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def read_server_log(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8").rstrip()
    except OSError:
        return ""


def format_starrocks_output(
    query_name: str,
    sql: str,
    status: str,
    elapsed_seconds: float | None,
    result_sets: Sequence[tuple[Sequence[str], Sequence[Sequence[object]]]],
    error: str | None,
) -> str:
    lines = [
        f"query={query_name}",
        f"status={status}",
    ]
    if elapsed_seconds is not None:
        lines.append(f"elapsed_seconds={format_seconds(elapsed_seconds)}")
    if error is not None:
        lines.append(f"error={error}")

    lines.extend(["", "sql:", sql.rstrip(), "", "output:"])
    if not result_sets:
        lines.append("(no result sets)")
    for result_set_index, (columns, rows) in enumerate(result_sets, start=1):
        lines.append(f"result_set={result_set_index}")
        lines.append(format_result_set(columns, rows))
    lines.append("")
    return "\n".join(lines)


def format_result_set(
    columns: Sequence[str], rows: Sequence[Sequence[object]]
) -> str:
    if not columns:
        return f"(no columns, {len(rows)} rows)"

    table_rows = [[format_sql_value(value) for value in row] for row in rows]
    widths = [
        max(len(str(column)), *(len(row[index]) for row in table_rows))
        for index, column in enumerate(columns)
    ]
    header = " | ".join(
        f"{column:<{width}}" for column, width in zip(columns, widths)
    )
    separator = "-+-".join("-" * width for width in widths)
    body = [
        " | ".join(f"{value:<{width}}" for value, width in zip(row, widths))
        for row in table_rows
    ]
    return "\n".join([header, separator, *body, f"({len(rows)} rows)"])


def format_sql_value(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid positive integer: {value}") from exc

    if parsed <= 0:
        raise argparse.ArgumentTypeError(f"invalid positive integer: {value}")
    return parsed


def normalize_query_name(query_name: str) -> str:
    query_name = query_name.strip()
    if query_name.endswith(".sql"):
        query_name = query_name[:-4]
    if not query_name:
        raise argparse.ArgumentTypeError("query name must not be empty")
    return query_name


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run Lakelet and AP database benchmarks.",
    )
    parser.add_argument(
        "--engine",
        choices=("lakelet", "starrocks"),
        required=True,
        help="Benchmark engine to run.",
    )
    parser.add_argument(
        "--benchmark-type",
        choices=SUPPORTED_BENCHMARK_TYPES,
        required=True,
        help="Benchmark type to run.",
    )
    parser.add_argument(
        "--default-catalog",
        required=True,
        help="Default catalog name for the benchmark session.",
    )
    parser.add_argument(
        "--default-schema",
        required=True,
        help="Default schema/database name for the benchmark session.",
    )
    parser.add_argument(
        "--runs",
        type=positive_int,
        default=RUNS_PER_QUERY,
        help=f"Runs per query. Defaults to {RUNS_PER_QUERY}.",
    )
    parser.add_argument(
        "--query",
        action="append",
        type=normalize_query_name,
        dest="queries",
        help="Run only the named query, for example q01. Can be specified multiple times.",
    )
    parser.add_argument(
        "--output",
        default="/tmp",
        help="Directory for benchmark output artifacts. Defaults to /tmp.",
    )

    lakelet = parser.add_argument_group("Lakelet connection")
    lakelet.add_argument("--bin", help="Path to the Lakelet binary.")
    lakelet.add_argument("--config", help="Path to the Lakelet config file.")

    starrocks = parser.add_argument_group("StarRocks connection")
    starrocks.add_argument("--host", help="StarRocks host.")
    starrocks.add_argument(
        "--port",
        type=positive_int,
        default=9030,
        help="StarRocks MySQL-compatible query port. Defaults to 9030.",
    )
    starrocks.add_argument("--user", help="StarRocks user.")
    starrocks.add_argument(
        "--password",
        help="StarRocks password. Defaults to STARROCKS_PASSWORD when omitted.",
    )

    return parser


def validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.engine == "lakelet":
        if not args.bin:
            parser.error("--bin is required when --engine lakelet")
        if not args.config:
            parser.error("--config is required when --engine lakelet")
    elif args.engine == "starrocks":
        if not args.host:
            parser.error("--host is required when --engine starrocks")
        if not args.user:
            parser.error("--user is required when --engine starrocks")


def build_runner(
    args: argparse.Namespace, repo_root: Path, run_dir: Path
) -> EngineRunner:
    if args.engine == "lakelet":
        return LakeletRunner(
            repo_root=repo_root,
            bin_path=Path(args.bin).expanduser().resolve(),
            config_path=Path(args.config).expanduser().resolve(),
            default_catalog=args.default_catalog,
            default_schema=args.default_schema,
            server_log_path=run_dir / "lakelet-server.log",
        )

    password = args.password
    if password is None:
        password = os.environ.get("STARROCKS_PASSWORD")

    return StarRocksRunner(
        host=args.host,
        port=args.port,
        user=args.user,
        password=password,
        default_catalog=args.default_catalog,
        default_schema=args.default_schema,
    )


def format_seconds(value: float) -> str:
    return f"{value:.3f}"


def summarize_times(times: Sequence[float]) -> tuple[float, float]:
    total_seconds = sum(times)
    average_seconds = total_seconds / len(times) if times else 0.0
    return total_seconds, average_seconds


def print_row(values: Sequence[str], widths: Sequence[int]) -> str:
    formatted = [
        f"{value:<{width}}" if index == 0 else f"{value:>{width}}"
        for index, (value, width) in enumerate(zip(values, widths))
    ]
    return " | ".join(formatted)


def print_separator(widths: Sequence[int]) -> str:
    return "-+-".join("-" * width for width in widths)


def log_results_header(output: BenchmarkOutput, runs: int) -> None:
    headers = ["query", *(f"run{index}(s)" for index in range(1, runs + 1)), "avg(s)"]
    widths = [8, *(10 for _ in range(runs)), 10]
    output.log(print_row(headers, widths))
    output.log(print_separator(widths))


def run_benchmark(args: argparse.Namespace, repo_root: Path) -> None:
    output = BenchmarkOutput.create(Path(args.output))
    runner = None

    try:
        suite = BenchmarkSuite(repo_root, args.benchmark_type, args.queries)
        runner = build_runner(args, repo_root, output.run_dir)
        all_times = []
        failed_runs = 0

        try:
            runner.prepare()
            log_results_header(output, args.runs)

            for sql_file in suite.sql_files:
                query_name = sql_file.stem
                run_values = []
                run_cells = []

                output.log(f"Running {query_name}...")
                for run_index in range(1, args.runs + 1):
                    try:
                        result = runner.run_query(sql_file)
                    except BenchmarkInfrastructureError:
                        raise
                    except BenchmarkError as exc:
                        failed_runs += 1
                        output.write_failure(query_name, run_index, exc)
                        output.log(
                            f"Skip {query_name} run {run_index}: benchmark failed: {exc}"
                        )
                        run_cells.append("FAILED")
                        continue
                    output.write_result(query_name, run_index, result)
                    run_values.append(result.elapsed_seconds)
                    run_cells.append(format_seconds(result.elapsed_seconds))
                    all_times.append(result.elapsed_seconds)

                average_seconds = sum(run_values) / len(run_values) if run_values else None
                row = [
                    query_name,
                    *run_cells,
                    "FAILED" if average_seconds is None else format_seconds(average_seconds),
                ]
                output.log(print_row(row, [8, *(10 for _ in range(args.runs)), 10]))
        finally:
            if runner is not None:
                runner.close()

        total_seconds, overall_average_seconds = summarize_times(all_times)

        output.log()
        output.log(f"Total queries: {len(suite.sql_files)}")
        output.log(f"Successful runs: {len(all_times)}")
        output.log(f"Failed runs: {failed_runs}")
        output.log(f"Total elapsed(s): {format_seconds(total_seconds)}")
        output.log(f"Average per run(s): {format_seconds(overall_average_seconds)}")
        output.log(f"Benchmark output: {output.run_dir}")
    except BenchmarkError:
        output.log()
        output.log(f"Benchmark output: {output.run_dir}")
        raise
    finally:
        output.close()


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    validate_args(parser, args)

    repo_root = Path(__file__).resolve().parent.parent
    try:
        run_benchmark(args, repo_root)
    except BenchmarkError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
