import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import boto3
import pytest
from moto.server import ThreadedMotoServer

E2E_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(E2E_ROOT))

from common.dobbydb import DobbyDbRunner
from common.testfile import E2ETestFile, load_test_file


AWS_ACCESS_KEY = "test"
AWS_SECRET_KEY = "test"
AWS_REGION = "us-east-1"
AWS_BUCKET = "dobbydb-e2e"
CATALOG_NAME = "moto_glue"
MOTO_HOST = "127.0.0.1"
MOTO_PORT = 5000
MOTO_ENDPOINT = f"http://{MOTO_HOST}:{MOTO_PORT}"
DOBBYDB_CONFIG = E2E_ROOT / "dobbydb-e2e.toml"
FORMAT_DATABASES = {
    "paimon": "paimon_db",
    "hive": "hive_db",
    "iceberg": "iceberg_db",
    "delta": "delta_db",
}


@dataclass(frozen=True)
class AwsContext:
    endpoint: str
    bucket: str
    s3_client: object
    glue_client: object


def _wait_for_moto(host: str, port: int) -> None:
    import socket

    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except OSError:
            time.sleep(0.2)
    raise RuntimeError(f"moto_server did not become ready: 127.0.0.1:{port}")


@pytest.fixture(scope="session")
def moto_endpoint():
    server = ThreadedMotoServer(ip_address=MOTO_HOST, port=MOTO_PORT, verbose=False)
    try:
        server.start()
        _wait_for_moto(MOTO_HOST, MOTO_PORT)
        yield MOTO_ENDPOINT
    finally:
        server.stop()


@pytest.fixture(scope="session")
def aws_context(moto_endpoint):
    client_args = {
        "endpoint_url": moto_endpoint,
        "region_name": AWS_REGION,
        "aws_access_key_id": AWS_ACCESS_KEY,
        "aws_secret_access_key": AWS_SECRET_KEY,
    }
    s3_client = boto3.client("s3", **client_args)
    glue_client = boto3.client("glue", **client_args)

    s3_client.create_bucket(Bucket=AWS_BUCKET)
    for database in FORMAT_DATABASES.values():
        glue_client.create_database(DatabaseInput={"Name": database})

    return AwsContext(
        endpoint=moto_endpoint,
        bucket=AWS_BUCKET,
        s3_client=s3_client,
        glue_client=glue_client,
    )


def pytest_generate_tests(metafunc):
    if "test_file" in metafunc.fixturenames:
        case_dir = metafunc.config.getoption("--e2e-case")
        test_paths = sorted((E2E_ROOT / "cases" / case_dir).glob("*.test"))
        metafunc.parametrize(
            "test_file",
            [load_test_file(path) for path in test_paths],
            ids=[path.stem for path in test_paths],
        )


def pytest_addoption(parser):
    parser.addoption("--e2e-case", default="paimon")


@pytest.fixture()
def dobbydb_runner(test_file: E2ETestFile):
    return DobbyDbRunner(
        bin_path=Path(os.environ["DOBBYDB_BIN"]),
        config_path=DOBBYDB_CONFIG,
        default_catalog=CATALOG_NAME,
        default_schema=test_file.database,
    )
