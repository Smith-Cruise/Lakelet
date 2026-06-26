import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class QueryResult:
    stdout: str
    header: list[str]
    rows: list[list[str]]


class DobbyDbRunner:
    def __init__(
        self,
        *,
        bin_path: Path,
        config_path: Path,
        default_catalog: str,
        default_schema: str,
    ) -> None:
        self.bin_path = bin_path
        self.config_path = config_path
        self.default_catalog = default_catalog
        self.default_schema = default_schema

    def query(self, sql: str) -> QueryResult:
        with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as sql_file:
            sql_file.write(sql)
            sql_path = Path(sql_file.name)
        try:
            command = [
                str(self.bin_path),
                "--config",
                str(self.config_path),
                "--default-catalog",
                self.default_catalog,
                "--default-schema",
                self.default_schema,
                "--file",
                str(sql_path),
            ]
            result = subprocess.run(
                command,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                check=False,
            )
            if result.returncode != 0:
                raise AssertionError(
                    f"DobbyDB query failed with exit code {result.returncode}\n"
                    f"SQL:\n{sql}\n\nOutput:\n{result.stdout}"
                )
            header, rows = parse_table_output(result.stdout)
            return QueryResult(stdout=result.stdout, header=header, rows=rows)
        finally:
            sql_path.unlink(missing_ok=True)


def parse_table_output(output: str) -> tuple[list[str], list[list[str]]]:
    header: list[str] | None = None
    rows: list[list[str]] = []
    for line in output.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|") or not stripped.endswith("|"):
            continue
        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        if header is None:
            header = cells
            continue
        rows.append(cells)
    return header or [], rows
