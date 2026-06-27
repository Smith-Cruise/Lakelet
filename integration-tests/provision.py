#!/usr/bin/env python3
import subprocess
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError


REPO_ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = REPO_ROOT / "integration-tests" / "docker-compose.yml"
HOST_ENDPOINT = "http://127.0.0.1:5050"
SPARK_ENDPOINT = "http://moto:5000"
REGION = "us-east-1"
ACCESS_KEY = "test"
SECRET_KEY = "test"
BUCKET = "dobbydb-e2e"
DATABASE = "paimon_db"
TABLE = "orders"
LOCATION = f"s3://{BUCKET}/warehouse/{DATABASE}.db/{TABLE}"
PAIMON_JARS = ",".join(
    [
        "/opt/spark/extra-jars/paimon-spark-4.0_2.13-1.4.1.jar",
        "/opt/spark/extra-jars/paimon-s3-1.4.1.jar",
    ]
)


def client(service: str):
    return boto3.client(
        service,
        endpoint_url=HOST_ENDPOINT,
        region_name=REGION,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
    )


def wait_for_moto() -> None:
    s3 = client("s3")
    deadline = time.monotonic() + 60
    while True:
        try:
            s3.list_buckets()
            return
        except EndpointConnectionError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(1)


def ensure_bucket() -> None:
    s3 = client("s3")
    try:
        s3.create_bucket(Bucket=BUCKET)
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code")
        if code not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
            raise


def ensure_database() -> None:
    glue = client("glue")
    try:
        glue.create_database(DatabaseInput={"Name": DATABASE})
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") != "AlreadyExistsException":
            raise


def create_paimon_data() -> None:
    subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(COMPOSE_FILE),
            "exec",
            "-T",
            "spark",
            "/opt/spark/bin/spark-sql",
            "--jars",
            PAIMON_JARS,
            "--conf",
            "spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions",
            "--conf",
            "spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog",
            "--conf",
            f"spark.sql.catalog.paimon.warehouse=s3://{BUCKET}/warehouse",
            "--conf",
            f"spark.sql.catalog.paimon.s3.endpoint={SPARK_ENDPOINT}",
            "--conf",
            f"spark.sql.catalog.paimon.s3.access-key={ACCESS_KEY}",
            "--conf",
            f"spark.sql.catalog.paimon.s3.secret-key={SECRET_KEY}",
            "--conf",
            "spark.sql.catalog.paimon.s3.path.style.access=true",
            "--conf",
            f"spark.sql.catalog.paimon.s3.region={REGION}",
            "-f",
            "/integration-tests/create-paimon-table.sql",
        ],
        check=True,
    )


def register_paimon_table() -> None:
    glue = client("glue")
    try:
        glue.delete_table(DatabaseName=DATABASE, Name=TABLE)
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") != "EntityNotFoundException":
            raise

    glue.create_table(
        DatabaseName=DATABASE,
        TableInput={
            "Name": TABLE,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                "EXTERNAL": "TRUE",
                "table_type": "PAIMON",
            },
            "StorageDescriptor": {
                "Columns": [],
                "Location": LOCATION,
                "InputFormat": "org.apache.paimon.hive.mapred.PaimonInputFormat",
                "OutputFormat": "org.apache.paimon.hive.mapred.PaimonOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.paimon.hive.PaimonSerDe",
                    "Parameters": {},
                },
            },
        },
    )


def main() -> None:
    wait_for_moto()
    ensure_bucket()
    ensure_database()
    create_paimon_data()
    register_paimon_table()


if __name__ == "__main__":
    main()
