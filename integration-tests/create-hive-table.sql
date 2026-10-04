DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
    id INT,
    name STRING,
    amount DECIMAL(10, 2),
    dt STRING
)
USING parquet
PARTITIONED BY (dt)
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/orders';

INSERT INTO orders
VALUES
    (1, 'alice', CAST(10.50 AS DECIMAL(10, 2)), '2026-06-25'),
    (2, 'bob', CAST(20.25 AS DECIMAL(10, 2)), '2026-06-25'),
    (3, 'carol', CAST(7.00 AS DECIMAL(10, 2)), '2026-06-24'),
    (4, 'dave', CAST(12.30 AS DECIMAL(10, 2)), '2026-06-24');

-- Hive-compatible legacy layout: lists as `bag`/`array_element` groups and maps
-- as `MAP_KEY_VALUE`. Names keep their case in the files, while Glue stores
-- them lowercase.
SET spark.sql.parquet.writeLegacyFormat=true;

DROP TABLE IF EXISTS nested_orders;

CREATE TABLE nested_orders (
    id INT,
    tags ARRAY<STRING>,
    attrs MAP<STRING, INT>,
    shipTo STRUCT<zipCode: STRING, cityName: STRING>,
    lineItems ARRAY<STRUCT<skuId: STRING, qty: INT>>
)
USING parquet
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/nested_orders';

INSERT INTO nested_orders
VALUES
    (
        1,
        array('a', 'b'),
        map('x', 1),
        named_struct('zipCode', '10001', 'cityName', 'nyc'),
        array(named_struct('skuId', 's1', 'qty', 2), named_struct('skuId', 's2', 'qty', 1))
    ),
    (
        2,
        CAST(array() AS ARRAY<STRING>),
        CAST(map() AS MAP<STRING, INT>),
        named_struct('zipCode', '94105', 'cityName', 'sf'),
        CAST(array() AS ARRAY<STRUCT<skuId: STRING, qty: INT>>)
    ),
    (3, NULL, NULL, NULL, NULL);
