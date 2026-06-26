#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys
from pathlib import Path
from urllib.request import urlretrieve


REPO_ROOT = Path(__file__).resolve().parents[1]
E2E_ROOT = Path(__file__).resolve().parent
PYTHON = Path("/Users/smith/Software/py-env/bin/python")
REQUIREMENTS = E2E_ROOT / "requirements.txt"
JAR_CACHE = E2E_ROOT / ".jars"

PAIMON_JAR_URL = (
    "https://repo.maven.apache.org/maven2/org/apache/paimon/"
    "paimon-spark-4.0_2.13/1.4.1/"
    "paimon-spark-4.0_2.13-1.4.1.jar"
)
PAIMON_JAR_NAME = "paimon-spark-4.0_2.13-1.4.1.jar"
PAIMON_S3_JAR_URL = (
    "https://repo.maven.apache.org/maven2/org/apache/paimon/"
    "paimon-s3/1.4.1/paimon-s3-1.4.1.jar"
)
PAIMON_S3_JAR_NAME = "paimon-s3-1.4.1.jar"


def run(command: list[str], *, env: dict[str, str] | None = None) -> None:
    subprocess.run(command, cwd=REPO_ROOT, env=env, check=True)


def ensure_python() -> None:
    if not PYTHON.is_file():
        raise SystemExit(f"Python env does not exist: {PYTHON}")

    current = Path(sys.executable).resolve()
    expected = PYTHON.resolve()
    if current != expected:
        raise SystemExit(
            f"Please run with {expected}, got {current}. "
            f"Use: source /Users/smith/Software/py-env/bin/activate"
        )


def install_requirements(skip_install: bool) -> None:
    if skip_install:
        return
    run([str(PYTHON), "-m", "pip", "install", "-r", str(REQUIREMENTS)])


def ensure_dobbydb() -> Path:
    bin_path = REPO_ROOT / "target" / "debug" / "dobbydb"
    if not bin_path.is_file():
        run(["cargo", "build", "--bin", "dobbydb"])
    return bin_path


def ensure_jar(url: str, name: str) -> Path:
    JAR_CACHE.mkdir(parents=True, exist_ok=True)
    jar_path = JAR_CACHE / name
    if not jar_path.is_file():
        urlretrieve(url, jar_path)
    return jar_path


def ensure_paimon_jars() -> list[Path]:
    return [
        ensure_jar(PAIMON_JAR_URL, PAIMON_JAR_NAME),
        ensure_jar(PAIMON_S3_JAR_URL, PAIMON_S3_JAR_NAME),
    ]


def pytest_target(case: str) -> Path:
    if case == "paimon":
        return E2E_ROOT / "test_e2e_files.py"
    raise SystemExit(f"Unsupported case for now: {case}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run DobbyDB end-to-end tests.")
    parser.add_argument(
        "--case",
        choices=["paimon", "hive", "iceberg", "delta", "all"],
        default="paimon",
        help="E2E case to run. Only paimon is implemented now.",
    )
    parser.add_argument(
        "--skip-install",
        action="store_true",
        help="Skip pip install from e2e-test/requirements.txt.",
    )
    args = parser.parse_args()

    if args.case != "paimon":
        raise SystemExit("Only --case paimon is implemented in this first version.")

    ensure_python()
    install_requirements(args.skip_install)
    dobbydb_bin = ensure_dobbydb()
    paimon_jars = ensure_paimon_jars()

    env = os.environ.copy()
    env["DOBBYDB_BIN"] = str(dobbydb_bin)
    env["PAIMON_SPARK_JARS"] = ",".join(str(path) for path in paimon_jars)
    env["AWS_ACCESS_KEY_ID"] = "test"
    env["AWS_SECRET_ACCESS_KEY"] = "test"
    env["AWS_DEFAULT_REGION"] = "us-east-1"
    env["PYSPARK_PYTHON"] = str(PYTHON)
    env["PYSPARK_DRIVER_PYTHON"] = str(PYTHON)

    run(
        [
            str(PYTHON),
            "-m",
            "pytest",
            "-q",
            str(pytest_target(args.case)),
            "--e2e-case",
            args.case,
        ],
        env=env,
    )


if __name__ == "__main__":
    main()
