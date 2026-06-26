import os
from pathlib import Path

from pyspark.sql import SparkSession


def create_spark_session(*, app_name: str, endpoint: str, warehouse: str) -> SparkSession:
    paimon_jars = os.environ["PAIMON_SPARK_JARS"]
    for jar in paimon_jars.split(","):
        jar_path = Path(jar)
        if not jar_path.is_file():
            raise AssertionError(f"Paimon Spark jar does not exist: {jar_path}")

    return (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.jars", paimon_jars)
        .config(
            "spark.sql.extensions",
            "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions",
        )
        .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
        .config("spark.sql.catalog.paimon.warehouse", warehouse)
        .config("spark.sql.catalog.paimon.s3.endpoint", endpoint)
        .config("spark.sql.catalog.paimon.s3.access-key", "test")
        .config("spark.sql.catalog.paimon.s3.secret-key", "test")
        .config("spark.sql.catalog.paimon.s3.path.style.access", "true")
        .config("spark.sql.catalog.paimon.s3.region", "us-east-1")
        .config("spark.pyspark.python", os.environ.get("PYSPARK_PYTHON", "python"))
        .config(
            "spark.pyspark.driver.python",
            os.environ.get("PYSPARK_DRIVER_PYTHON", "python"),
        )
        .getOrCreate()
    )
