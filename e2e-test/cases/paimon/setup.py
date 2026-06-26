from common.glue import create_paimon_table
from common.spark import create_spark_session


PAIMON_DATABASE = "paimon_db"
ORDERS_TABLE = "orders"


def setup_orders(aws_context) -> None:
    warehouse = f"s3://{aws_context.bucket}/warehouse"
    table_location = f"{warehouse}/{PAIMON_DATABASE}.db/{ORDERS_TABLE}"

    spark = create_spark_session(
        app_name="dobbydb-paimon-e2e",
        endpoint=aws_context.endpoint,
        warehouse=warehouse,
    )
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS paimon.`{PAIMON_DATABASE}`")
        spark.sql(f"DROP TABLE IF EXISTS paimon.`{PAIMON_DATABASE}`.`{ORDERS_TABLE}`")
        spark.sql(
            f"""
            CREATE TABLE paimon.`{PAIMON_DATABASE}`.`{ORDERS_TABLE}` (
                id INT,
                name STRING,
                amount DECIMAL(10, 2),
                dt STRING
            )
            PARTITIONED BY (dt)
            TBLPROPERTIES (
                'bucket' = '-1'
            )
            """
        )
        spark.sql(
            f"""
            INSERT INTO paimon.`{PAIMON_DATABASE}`.`{ORDERS_TABLE}`
            VALUES
                (1, 'alice', CAST(10.50 AS DECIMAL(10, 2)), '2026-06-25'),
                (2, 'bob', CAST(20.25 AS DECIMAL(10, 2)), '2026-06-25'),
                (3, 'carol', CAST(7.00 AS DECIMAL(10, 2)), '2026-06-24'),
                (4, 'dave', CAST(12.30 AS DECIMAL(10, 2)), '2026-06-24')
            """
        )
    finally:
        spark.stop()

    try:
        aws_context.glue_client.delete_table(
            DatabaseName=PAIMON_DATABASE,
            Name=ORDERS_TABLE,
        )
    except aws_context.glue_client.exceptions.EntityNotFoundException:
        pass

    create_paimon_table(
        glue_client=aws_context.glue_client,
        database=PAIMON_DATABASE,
        table_name=ORDERS_TABLE,
        location=table_location,
    )
