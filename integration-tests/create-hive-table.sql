SET spark.sql.session.timeZone=UTC;
SET spark.sql.shuffle.partitions=1;

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

-- Reuse the Parquet rows to compare both columnar Hive readers.
DROP TABLE IF EXISTS orc_orders;
CREATE TABLE orc_orders
USING orc
PARTITIONED BY (dt)
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/orc_orders'
AS SELECT * FROM orders;

DROP TABLE IF EXISTS orc_nested_orders;
CREATE TABLE orc_nested_orders
USING orc
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/orc_nested_orders'
AS SELECT * FROM nested_orders;

-- Exercise the Hive Avro SerDe writer rather than a Data Source Avro table.
DROP TABLE IF EXISTS avro_orders;
CREATE EXTERNAL TABLE avro_orders (
    id INT,
    name STRING,
    amount DECIMAL(10, 2)
)
PARTITIONED BY (dt STRING)
STORED AS AVRO
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/avro_orders';

INSERT INTO avro_orders PARTITION (dt = '2026-06-25')
SELECT id, name, amount FROM orders WHERE dt = '2026-06-25';

INSERT INTO avro_orders PARTITION (dt = '2026-06-24')
SELECT id, name, amount FROM orders WHERE dt = '2026-06-24';


-- Keep Hive fixtures deterministic across machines and Spark sessions.
SET spark.sql.parquet.int96RebaseModeInWrite=CORRECTED;
SET spark.sql.parquet.datetimeRebaseModeInWrite=CORRECTED;

CREATE OR REPLACE TEMP VIEW scalar_rows AS
SELECT * FROM VALUES
 (1, -128, -32768, -2147483648, -9223372036854775808L, -1.5, -2.25, true,
  '-10.50', '-99999999999999999999999999999999999999', '-12345678901234567890.123456789012345678',
  '中文', 'ab', 'value', X'00FF41', DATE '1960-01-02', TIMESTAMP '1960-01-02 03:04:05.123456'),
 (2, 127, 32767, 2147483647, 9223372036854775807L, 1.5, 2.25, false,
  '99999999.99', '99999999999999999999999999999999999999', '12345678901234567890.123456789012345678',
  '', 'abcde', '', X'', DATE '2000-02-29', TIMESTAMP '2000-02-29 00:00:00.000001'),
 (3, 0, 0, 0, 9007199254740993L, 0.0, 0.0, NULL,
  '0.00', '0', '0.000000000000000000',
  '  ', 'a b', 'quote"', X'4100', DATE '1970-01-01', TIMESTAMP '1969-12-31 23:59:59.999999'),
 (4, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
AS r(id, tiny, small, integer_col, big, real_col, double_col, flag, amount, wide, fraction,
     name, fixed, variable, bytes_col, day, ts);

-- Add cases that distinguish NULL containers from NULL children.
INSERT INTO nested_orders VALUES
 (4, array(NULL, 'z'), map('x', CAST(NULL AS INT)),
  named_struct('zipCode', CAST(NULL AS STRING), 'cityName', 'sf'),
  array(CAST(NULL AS STRUCT<skuId:STRING,qty:INT>), named_struct('skuId', 's3', 'qty', CAST(NULL AS INT)))),
 (5, array(''), map('x', 2),
  named_struct('zipCode', CAST(NULL AS STRING), 'cityName', CAST(NULL AS STRING)),
  CAST(array() AS ARRAY<STRUCT<skuId:STRING,qty:INT>>)),
 (6, NULL, map('x', 3), NULL, array(named_struct('skuId', 's4', 'qty', 4)));
INSERT INTO orc_nested_orders SELECT * FROM nested_orders WHERE id >= 4;

CREATE OR REPLACE TEMP VIEW partition_rows AS
SELECT * FROM VALUES
 (1, 'alice', 10.50, DATE '2026-06-24', 2, 'CN', 2.00),
 (2, 'bob', 20.25, DATE '2026-06-24', 10, 'US', 10.00),
 (3, 'carol', 7.00, DATE '2026-06-25', 2, 'CN', 2.00),
 (4, 'dave', 12.30, DATE '2026-06-25', 10, 'US', 10.00),
 (5, '中文', 0.00, DATE '2026-06-26', 2, '中文/=%', 2.00),
 (6, 'custom', -1.00, DATE '2026-06-26', 10, 'US', 10.00),
 (7, NULL, NULL, NULL, NULL, NULL, NULL),
 (99, 'unregistered', 99.00, DATE '2026-06-27', 2, 'ghost', 2.00)
AS r(id, name, amount, dt, bucket, region, price);


-- PARQUET: the same values exercise a different Hive reader.

DROP TABLE IF EXISTS parquet_types;
CREATE TABLE parquet_types (
    id INT,
    tiny TINYINT,
    small SMALLINT,
    integer_col INT,
    big BIGINT,
    real_col FLOAT,
    double_col DOUBLE,
    flag BOOLEAN,
    amount DECIMAL(10,2),
    wide DECIMAL(38,0),
    fraction DECIMAL(38,18),
    name STRING,
    fixed CHAR(5),
    variable VARCHAR(10),
    bytes_col BINARY,
    day DATE,
    ts TIMESTAMP
)
USING parquet
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/parquet_types';

INSERT INTO parquet_types
SELECT id, CAST(tiny AS TINYINT), CAST(small AS SMALLINT), integer_col, big,
       CAST(real_col AS FLOAT), CAST(double_col AS DOUBLE), flag,
       CAST(amount AS DECIMAL(10,2)), CAST(wide AS DECIMAL(38,0)), CAST(fraction AS DECIMAL(38,18)),
       name, fixed, variable, bytes_col, day, ts
FROM scalar_rows;

DROP TABLE IF EXISTS parquet_partitions;
CREATE TABLE parquet_partitions (
    id INT,
    name STRING,
    amount DECIMAL(10,2),
    dt DATE,
    bucket INT,
    region STRING,
    price DECIMAL(6,2)
)
USING parquet
PARTITIONED BY (dt, bucket, region, price)
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/parquet_partitions';

INSERT INTO parquet_partitions SELECT * FROM partition_rows;

DROP TABLE IF EXISTS parquet_custom_partition;
CREATE TABLE parquet_custom_partition (
    id INT,
    name STRING,
    amount DECIMAL(10,2)
)
USING parquet
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/parquet_custom_partition';

INSERT INTO parquet_custom_partition SELECT id, name, amount FROM partition_rows WHERE id = 6;

DROP TABLE IF EXISTS parquet_scan;
CREATE TABLE parquet_scan (
    id INT,
    name STRING,
    flag BOOLEAN,
    amount DECIMAL(10,2),
    nullable INT
)
USING parquet
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/parquet_scan';

INSERT INTO parquet_scan
SELECT CAST(id AS INT), CASE WHEN id % 7 = 0 THEN '目标' ELSE 'row' END,
       id % 2 = 0, CAST(id / 100.0 AS DECIMAL(10,2)),
       CASE WHEN id % 5 = 0 THEN NULL ELSE CAST(id AS INT) END
FROM range(0, 2000, 1, 1);

INSERT INTO parquet_scan
SELECT CAST(id AS INT), CASE WHEN id % 7 = 0 THEN '目标' ELSE 'row' END,
       id % 2 = 0, CAST(id / 100.0 AS DECIMAL(10,2)),
       CASE WHEN id % 5 = 0 THEN NULL ELSE CAST(id AS INT) END
FROM range(2000, 4000, 1, 1);

INSERT INTO parquet_scan
SELECT CAST(id AS INT), CASE WHEN id % 7 = 0 THEN '目标' ELSE 'row' END,
       id % 2 = 0, CAST(id / 100.0 AS DECIMAL(10,2)),
       CASE WHEN id % 5 = 0 THEN NULL ELSE CAST(id AS INT) END
FROM range(4000, 6000, 1, 1);

DROP TABLE IF EXISTS parquet_evolution;
CREATE TABLE parquet_evolution (
    id INT,
    name STRING
)
USING parquet
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/parquet_evolution';

INSERT INTO parquet_evolution VALUES (1, 'old');
ALTER TABLE parquet_evolution ADD COLUMNS (extra INT);
INSERT INTO parquet_evolution VALUES (2, 'new', 20);

DROP TABLE IF EXISTS parquet_adaptation_old;
CREATE TABLE parquet_adaptation_old (
    unused STRING,
    UserId INT,
    Name STRING,
    Info STRUCT<Value:INT,Label:STRING>
)
USING parquet
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/parquet_adaptation/old';

INSERT INTO parquet_adaptation_old VALUES ('ignored', 1, 'old', named_struct('value', 10, 'label', 'v1'));

DROP TABLE IF EXISTS parquet_adaptation_new;
CREATE TABLE parquet_adaptation_new (
    name STRING,
    userid BIGINT,
    info STRUCT<label:STRING,value:BIGINT>,
    unused INT
)
USING parquet
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/parquet_adaptation/new';

INSERT INTO parquet_adaptation_new VALUES ('new', 2L, named_struct('label', 'v2', 'value', 20L), 99);

SELECT assert_true(count(*) = 1 AND min(userid) = 1 AND min(info.value) = 10) FROM parquet_adaptation_old;
SELECT assert_true(count(*) = 1 AND min(userid) = 2 AND min(info.value) = 20) FROM parquet_adaptation_new;

DROP TABLE IF EXISTS parquet_empty;
CREATE TABLE parquet_empty (
    id INT,
    name STRING
)
USING parquet
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/parquet_empty';

DROP TABLE IF EXISTS parquet_hidden;
CREATE TABLE parquet_hidden (
    id INT,
    name STRING
)
USING parquet
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/parquet_scan/_hidden';

INSERT INTO parquet_hidden VALUES (99999, 'hidden');

SELECT assert_true(count(*) = 4) FROM parquet_types;
SELECT assert_true(big = -9223372036854775808L AND hex(bytes_col) = '00FF41'
                   AND name = '中文' AND amount = -10.50
                   AND ts = TIMESTAMP '1960-01-02 03:04:05.123456') FROM parquet_types WHERE id = 1;
SELECT assert_true(big = 9007199254740993L AND length(name) = 2
                   AND ts = TIMESTAMP '1969-12-31 23:59:59.999999') FROM parquet_types WHERE id = 3;
SELECT assert_true(count(*) = 6000) FROM parquet_scan;
SELECT assert_true(count(*) = 2 AND count(extra) = 1) FROM parquet_evolution;
SELECT assert_true(count(*) = 8) FROM parquet_partitions;

SELECT assert_true(count(*) = 6 AND count(tags) = 4 AND count(attrs) = 5
                   AND count(shipto) = 4 AND count(lineitems) = 5) FROM nested_orders;
SELECT assert_true(size(tags) = 0 AND size(attrs) = 0 AND size(lineitems) = 0)
FROM nested_orders WHERE id = 2;


-- ORC: the same values exercise a different Hive reader.

DROP TABLE IF EXISTS orc_types;
CREATE TABLE orc_types (
    id INT,
    tiny TINYINT,
    small SMALLINT,
    integer_col INT,
    big BIGINT,
    real_col FLOAT,
    double_col DOUBLE,
    flag BOOLEAN,
    amount DECIMAL(10,2),
    wide DECIMAL(38,0),
    fraction DECIMAL(38,18),
    name STRING,
    fixed CHAR(5),
    variable VARCHAR(10),
    bytes_col BINARY,
    day DATE,
    ts TIMESTAMP
)
USING orc
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/orc_types';

-- Use a negative fractional timestamp that Spark ORC can round-trip.

INSERT INTO orc_types
SELECT id, CAST(tiny AS TINYINT), CAST(small AS SMALLINT), integer_col, big,
       CAST(real_col AS FLOAT), CAST(double_col AS DOUBLE), flag,
       CAST(amount AS DECIMAL(10,2)), CAST(wide AS DECIMAL(38,0)), CAST(fraction AS DECIMAL(38,18)),
       name, fixed, variable, bytes_col, day, CASE WHEN id = 3 THEN TIMESTAMP '1969-12-31 23:59:58.999999' ELSE ts END
FROM scalar_rows;

DROP TABLE IF EXISTS orc_partitions;
CREATE TABLE orc_partitions (
    id INT,
    name STRING,
    amount DECIMAL(10,2),
    dt DATE,
    bucket INT,
    region STRING,
    price DECIMAL(6,2)
)
USING orc
PARTITIONED BY (dt, bucket, region, price)
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/orc_partitions';

INSERT INTO orc_partitions SELECT * FROM partition_rows;

DROP TABLE IF EXISTS orc_custom_partition;
CREATE TABLE orc_custom_partition (
    id INT,
    name STRING,
    amount DECIMAL(10,2)
)
USING orc
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/orc_custom_partition';

INSERT INTO orc_custom_partition SELECT id, name, amount FROM partition_rows WHERE id = 6;

DROP TABLE IF EXISTS orc_scan;
CREATE TABLE orc_scan (
    id INT,
    name STRING,
    flag BOOLEAN,
    amount DECIMAL(10,2),
    nullable INT
)
USING orc
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/orc_scan';

INSERT INTO orc_scan
SELECT CAST(id AS INT), CASE WHEN id % 7 = 0 THEN '目标' ELSE 'row' END,
       id % 2 = 0, CAST(id / 100.0 AS DECIMAL(10,2)),
       CASE WHEN id % 5 = 0 THEN NULL ELSE CAST(id AS INT) END
FROM range(0, 2000, 1, 1);

INSERT INTO orc_scan
SELECT CAST(id AS INT), CASE WHEN id % 7 = 0 THEN '目标' ELSE 'row' END,
       id % 2 = 0, CAST(id / 100.0 AS DECIMAL(10,2)),
       CASE WHEN id % 5 = 0 THEN NULL ELSE CAST(id AS INT) END
FROM range(2000, 4000, 1, 1);

INSERT INTO orc_scan
SELECT CAST(id AS INT), CASE WHEN id % 7 = 0 THEN '目标' ELSE 'row' END,
       id % 2 = 0, CAST(id / 100.0 AS DECIMAL(10,2)),
       CASE WHEN id % 5 = 0 THEN NULL ELSE CAST(id AS INT) END
FROM range(4000, 6000, 1, 1);

DROP TABLE IF EXISTS orc_evolution;
CREATE TABLE orc_evolution (
    id INT,
    name STRING
)
USING orc
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/orc_evolution';

INSERT INTO orc_evolution VALUES (1, 'old');
ALTER TABLE orc_evolution ADD COLUMNS (extra INT);
INSERT INTO orc_evolution VALUES (2, 'new', 20);

DROP TABLE IF EXISTS orc_adaptation_old;
CREATE TABLE orc_adaptation_old (
    unused STRING,
    UserId INT,
    Name STRING,
    Info STRUCT<Value:INT,Label:STRING>
)
USING orc
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/orc_adaptation/old';

INSERT INTO orc_adaptation_old VALUES ('ignored', 1, 'old', named_struct('value', 10, 'label', 'v1'));

DROP TABLE IF EXISTS orc_adaptation_new;
CREATE TABLE orc_adaptation_new (
    name STRING,
    userid BIGINT,
    info STRUCT<label:STRING,value:BIGINT>,
    unused INT
)
USING orc
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/orc_adaptation/new';

INSERT INTO orc_adaptation_new VALUES ('new', 2L, named_struct('label', 'v2', 'value', 20L), 99);

SELECT assert_true(count(*) = 1 AND min(userid) = 1 AND min(info.value) = 10) FROM orc_adaptation_old;
SELECT assert_true(count(*) = 1 AND min(userid) = 2 AND min(info.value) = 20) FROM orc_adaptation_new;

DROP TABLE IF EXISTS orc_empty;
CREATE TABLE orc_empty (
    id INT,
    name STRING
)
USING orc
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/orc_empty';

DROP TABLE IF EXISTS orc_hidden;
CREATE TABLE orc_hidden (
    id INT,
    name STRING
)
USING orc
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/orc_scan/_hidden';

INSERT INTO orc_hidden VALUES (99999, 'hidden');

SELECT assert_true(count(*) = 4) FROM orc_types;
SELECT assert_true(big = -9223372036854775808L AND hex(bytes_col) = '00FF41'
                   AND name = '中文' AND amount = -10.50
                   AND ts = TIMESTAMP '1960-01-02 03:04:05.123456') FROM orc_types WHERE id = 1;
SELECT assert_true(big = 9007199254740993L AND length(name) = 2
                   AND ts = TIMESTAMP '1969-12-31 23:59:58.999999') FROM orc_types WHERE id = 3;
SELECT assert_true(count(*) = 6000) FROM orc_scan;
SELECT assert_true(count(*) = 2 AND count(extra) = 1) FROM orc_evolution;
SELECT assert_true(count(*) = 8) FROM orc_partitions;

SELECT assert_true(count(*) = 6 AND count(tags) = 4 AND count(attrs) = 5
                   AND count(shipto) = 4 AND count(lineitems) = 5) FROM orc_nested_orders;
SELECT assert_true(size(tags) = 0 AND size(attrs) = 0 AND size(lineitems) = 0)
FROM orc_nested_orders WHERE id = 2;


-- AVRO: the same values exercise a different Hive reader.

DROP TABLE IF EXISTS avro_types;
CREATE EXTERNAL TABLE avro_types (
    id INT,
    tiny TINYINT,
    small SMALLINT,
    integer_col INT,
    big BIGINT,
    real_col FLOAT,
    double_col DOUBLE,
    flag BOOLEAN,
    amount DECIMAL(10,2),
    wide DECIMAL(38,0),
    fraction DECIMAL(38,18),
    name STRING,
    fixed CHAR(5),
    variable VARCHAR(10),
    bytes_col BINARY,
    day DATE,
    ts TIMESTAMP,
    void_col VOID
)
STORED AS AVRO
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/avro_types';

INSERT INTO avro_types
SELECT id, CAST(tiny AS TINYINT), CAST(small AS SMALLINT), integer_col, big,
       CAST(real_col AS FLOAT), CAST(double_col AS DOUBLE), flag,
       CAST(amount AS DECIMAL(10,2)), CAST(wide AS DECIMAL(38,0)), CAST(fraction AS DECIMAL(38,18)),
       name, fixed, variable, bytes_col, day, ts, CAST(NULL AS VOID)
FROM scalar_rows;

DROP TABLE IF EXISTS avro_nested_orders;
CREATE EXTERNAL TABLE avro_nested_orders (
    id INT,
    tags ARRAY<STRING>,
    attrs MAP<STRING,INT>,
    shipto STRUCT<zipcode:STRING,cityname:STRING>,
    lineitems ARRAY<STRUCT<skuid:STRING,qty:INT>>
)
STORED AS AVRO
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/avro_nested_orders';

INSERT INTO avro_nested_orders SELECT * FROM nested_orders;

DROP TABLE IF EXISTS avro_partitions;
CREATE EXTERNAL TABLE avro_partitions (
    id INT,
    name STRING,
    amount DECIMAL(10,2)
)
PARTITIONED BY (
    dt DATE,
    bucket INT,
    region STRING,
    price DECIMAL(6,2)
)
STORED AS AVRO
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/avro_partitions';

INSERT INTO avro_partitions SELECT * FROM partition_rows;

DROP TABLE IF EXISTS avro_custom_partition;
CREATE EXTERNAL TABLE avro_custom_partition (
    id INT,
    name STRING,
    amount DECIMAL(10,2)
)
STORED AS AVRO
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/avro_custom_partition';

INSERT INTO avro_custom_partition SELECT id, name, amount FROM partition_rows WHERE id = 6;

DROP TABLE IF EXISTS avro_scan;
CREATE EXTERNAL TABLE avro_scan (
    id INT,
    name STRING,
    flag BOOLEAN,
    amount DECIMAL(10,2),
    nullable INT
)
STORED AS AVRO
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/avro_scan';

INSERT INTO avro_scan
SELECT CAST(id AS INT), CASE WHEN id % 7 = 0 THEN '目标' ELSE 'row' END,
       id % 2 = 0, CAST(id / 100.0 AS DECIMAL(10,2)),
       CASE WHEN id % 5 = 0 THEN NULL ELSE CAST(id AS INT) END
FROM range(0, 2000, 1, 1);

INSERT INTO avro_scan
SELECT CAST(id AS INT), CASE WHEN id % 7 = 0 THEN '目标' ELSE 'row' END,
       id % 2 = 0, CAST(id / 100.0 AS DECIMAL(10,2)),
       CASE WHEN id % 5 = 0 THEN NULL ELSE CAST(id AS INT) END
FROM range(2000, 4000, 1, 1);

INSERT INTO avro_scan
SELECT CAST(id AS INT), CASE WHEN id % 7 = 0 THEN '目标' ELSE 'row' END,
       id % 2 = 0, CAST(id / 100.0 AS DECIMAL(10,2)),
       CASE WHEN id % 5 = 0 THEN NULL ELSE CAST(id AS INT) END
FROM range(4000, 6000, 1, 1);

DROP TABLE IF EXISTS avro_evolution;
CREATE EXTERNAL TABLE avro_evolution (
    id INT,
    name STRING
)
STORED AS AVRO
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/avro_evolution';

INSERT INTO avro_evolution VALUES (1, 'old');
ALTER TABLE avro_evolution ADD COLUMNS (extra INT);
INSERT INTO avro_evolution VALUES (2, 'new', 20);

DROP TABLE IF EXISTS avro_adaptation_old;
CREATE EXTERNAL TABLE avro_adaptation_old (
    unused string,
    userid int,
    name string,
    info struct<value:int,label:string>
)
STORED AS AVRO
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/avro_adaptation/old';

INSERT INTO avro_adaptation_old VALUES ('ignored', 1, 'old', named_struct('value', 10, 'label', 'v1'));

DROP TABLE IF EXISTS avro_adaptation_new;
CREATE EXTERNAL TABLE avro_adaptation_new (
    name STRING,
    userid BIGINT,
    info STRUCT<label:STRING,value:BIGINT>,
    unused INT
)
STORED AS AVRO
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/avro_adaptation/new';

INSERT INTO avro_adaptation_new VALUES ('new', 2L, named_struct('label', 'v2', 'value', 20L), 99);

SELECT assert_true(count(*) = 1 AND min(userid) = 1 AND min(info.value) = 10) FROM avro_adaptation_old;
SELECT assert_true(count(*) = 1 AND min(userid) = 2 AND min(info.value) = 20) FROM avro_adaptation_new;

DROP TABLE IF EXISTS avro_empty;
CREATE EXTERNAL TABLE avro_empty (
    id INT,
    name STRING
)
STORED AS AVRO
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/avro_empty';

DROP TABLE IF EXISTS avro_hidden;
CREATE EXTERNAL TABLE avro_hidden (
    id INT,
    name STRING
)
STORED AS AVRO
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/avro_scan/_hidden';

INSERT INTO avro_hidden VALUES (99999, 'hidden');

SELECT assert_true(count(*) = 4) FROM avro_types;
SELECT assert_true(big = -9223372036854775808L AND hex(bytes_col) = '00FF41'
                   AND name = '中文' AND amount = -10.50
                   AND ts = TIMESTAMP '1960-01-02 03:04:05.123') FROM avro_types WHERE id = 1;
SELECT assert_true(big = 9007199254740993L AND length(name) = 2
                   AND ts = TIMESTAMP '1969-12-31 23:59:59.999') FROM avro_types WHERE id = 3;
SELECT assert_true(count(*) = 6000) FROM avro_scan;
SELECT assert_true(count(*) = 2 AND count(extra) = 1) FROM avro_evolution;
SELECT assert_true(count(*) = 8) FROM avro_partitions;

-- Hive Avro serializes CHAR without padding and timestamps as milliseconds.
CREATE OR REPLACE TEMP VIEW avro_physical USING avro
OPTIONS (path 's3://lakelet-e2e/warehouse/hive_db.db/avro_types');
SELECT assert_true(length(fixed) = 2 AND ts = TIMESTAMP '1960-01-02 03:04:05.123')
FROM avro_physical WHERE id = 1;

SELECT assert_true(count(*) = 4 AND count(void_col) = 0) FROM avro_types;

SELECT assert_true(count(*) = 6 AND count(tags) = 4 AND count(attrs) = 5
                   AND count(shipto) = 4 AND count(lineitems) = 5) FROM avro_nested_orders;
SELECT assert_true(size(tags) = 0 AND size(attrs) = 0 AND size(lineitems) = 0)
FROM avro_nested_orders WHERE id = 2;


-- TEXTFILE: the same values exercise a different Hive reader.

DROP TABLE IF EXISTS textfile_types;
CREATE EXTERNAL TABLE textfile_types (
    id INT,
    tiny TINYINT,
    small SMALLINT,
    integer_col INT,
    big BIGINT,
    real_col FLOAT,
    double_col DOUBLE,
    flag BOOLEAN,
    amount DECIMAL(10,2),
    wide DECIMAL(38,0),
    fraction DECIMAL(38,18),
    name STRING,
    fixed CHAR(5),
    variable VARCHAR(10),
    bytes_col BINARY,
    day DATE,
    ts TIMESTAMP,
    void_col VOID
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS TEXTFILE
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/textfile_types';

INSERT INTO textfile_types
SELECT id, CAST(tiny AS TINYINT), CAST(small AS SMALLINT), integer_col, big,
       CAST(real_col AS FLOAT), CAST(double_col AS DOUBLE), flag,
       CAST(amount AS DECIMAL(10,2)), CAST(wide AS DECIMAL(38,0)), CAST(fraction AS DECIMAL(38,18)),
       name, fixed, variable, bytes_col, day, ts, CAST(NULL AS VOID)
FROM scalar_rows;

DROP TABLE IF EXISTS textfile_nested_orders;
CREATE EXTERNAL TABLE textfile_nested_orders (
    id INT,
    tags ARRAY<STRING>,
    attrs MAP<STRING,INT>,
    shipto STRUCT<zipcode:STRING,cityname:STRING>,
    lineitems ARRAY<STRUCT<skuid:STRING,qty:INT>>
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS TEXTFILE
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/textfile_nested_orders';

INSERT INTO textfile_nested_orders SELECT * FROM nested_orders;

DROP TABLE IF EXISTS textfile_partitions;
CREATE EXTERNAL TABLE textfile_partitions (
    id INT,
    name STRING,
    amount DECIMAL(10,2)
)
PARTITIONED BY (
    dt DATE,
    bucket INT,
    region STRING,
    price DECIMAL(6,2)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS TEXTFILE
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/textfile_partitions';

INSERT INTO textfile_partitions SELECT * FROM partition_rows;

DROP TABLE IF EXISTS textfile_custom_partition;
CREATE EXTERNAL TABLE textfile_custom_partition (
    id INT,
    name STRING,
    amount DECIMAL(10,2)
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS TEXTFILE
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/textfile_custom_partition';

INSERT INTO textfile_custom_partition SELECT id, name, amount FROM partition_rows WHERE id = 6;

DROP TABLE IF EXISTS textfile_scan;
CREATE EXTERNAL TABLE textfile_scan (
    id INT,
    name STRING,
    flag BOOLEAN,
    amount DECIMAL(10,2),
    nullable INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS TEXTFILE
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/textfile_scan';

INSERT INTO textfile_scan
SELECT CAST(id AS INT), CASE WHEN id % 7 = 0 THEN '目标' ELSE 'row' END,
       id % 2 = 0, CAST(id / 100.0 AS DECIMAL(10,2)),
       CASE WHEN id % 5 = 0 THEN NULL ELSE CAST(id AS INT) END
FROM range(0, 2000, 1, 1);

INSERT INTO textfile_scan
SELECT CAST(id AS INT), CASE WHEN id % 7 = 0 THEN '目标' ELSE 'row' END,
       id % 2 = 0, CAST(id / 100.0 AS DECIMAL(10,2)),
       CASE WHEN id % 5 = 0 THEN NULL ELSE CAST(id AS INT) END
FROM range(2000, 4000, 1, 1);

INSERT INTO textfile_scan
SELECT CAST(id AS INT), CASE WHEN id % 7 = 0 THEN '目标' ELSE 'row' END,
       id % 2 = 0, CAST(id / 100.0 AS DECIMAL(10,2)),
       CASE WHEN id % 5 = 0 THEN NULL ELSE CAST(id AS INT) END
FROM range(4000, 6000, 1, 1);

DROP TABLE IF EXISTS textfile_evolution;
CREATE EXTERNAL TABLE textfile_evolution (
    id INT,
    name STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS TEXTFILE
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/textfile_evolution';

INSERT INTO textfile_evolution VALUES (1, 'old');
ALTER TABLE textfile_evolution ADD COLUMNS (extra INT);
INSERT INTO textfile_evolution VALUES (2, 'new', 20);

DROP TABLE IF EXISTS textfile_empty;
CREATE EXTERNAL TABLE textfile_empty (
    id INT,
    name STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS TEXTFILE
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/textfile_empty';

DROP TABLE IF EXISTS textfile_hidden;
CREATE EXTERNAL TABLE textfile_hidden (
    id INT,
    name STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS TEXTFILE
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/textfile_scan/_hidden';

INSERT INTO textfile_hidden VALUES (99999, 'hidden');

SELECT assert_true(count(*) = 4) FROM textfile_types;
SELECT assert_true(big = -9223372036854775808L AND hex(bytes_col) = '00FF41'
                   AND name = '中文' AND amount = -10.50
                   AND ts = TIMESTAMP '1960-01-02 03:04:05.123456') FROM textfile_types WHERE id = 1;
SELECT assert_true(big = 9007199254740993L AND length(name) = 2
                   AND ts = TIMESTAMP '1969-12-31 23:59:59.999999') FROM textfile_types WHERE id = 3;
SELECT assert_true(count(*) = 6000) FROM textfile_scan;
SELECT assert_true(count(*) = 2 AND count(extra) = 1) FROM textfile_evolution;
SELECT assert_true(count(*) = 8) FROM textfile_partitions;

SELECT assert_true(count(*) = 4 AND count(void_col) = 0) FROM textfile_types;

SELECT assert_true(count(*) = 6 AND count(tags) = 4 AND count(attrs) = 5
                   AND count(shipto) = 4 AND count(lineitems) = 5) FROM textfile_nested_orders;
SELECT assert_true(size(tags) = 0 AND size(attrs) = 0 AND size(lineitems) = 0)
FROM textfile_nested_orders WHERE id = 2;


-- Modern nested Parquet layout is written separately from the legacy fixture.
SET spark.sql.parquet.writeLegacyFormat=false;
DROP TABLE IF EXISTS parquet_modern_nested;
CREATE TABLE parquet_modern_nested USING parquet
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/parquet_modern_nested'
AS SELECT * FROM nested_orders;

SET spark.sql.parquet.outputTimestampType=INT96;
DROP TABLE IF EXISTS parquet_int96;
CREATE TABLE parquet_int96 USING parquet
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/parquet_int96'
AS SELECT 1 AS id, TIMESTAMP '1969-12-31 23:59:59.999999' AS ts
UNION ALL SELECT 2, TIMESTAMP '2500-01-01 00:00:00.123456';

-- Use Spark write options to exercise a larger uncompressed payload.
SET parquet.block.size=16384;
DROP TABLE IF EXISTS parquet_pruning;
CREATE TABLE parquet_pruning USING parquet OPTIONS (compression 'uncompressed')
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/parquet_pruning'
AS SELECT CAST(id AS INT) AS id, repeat(CAST(id AS STRING), 128) AS payload
FROM range(6000) ORDER BY id;
RESET parquet.block.size;
RESET spark.sql.parquet.outputTimestampType;
RESET spark.sql.parquet.writeLegacyFormat;

-- TextFile parsing properties are exercised with Spark-written text values.
DROP TABLE IF EXISTS textfile_custom;
CREATE EXTERNAL TABLE textfile_custom (id STRING, name STRING, amount STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'='|', 'serialization.null.format'='NULL')
STORED AS TEXTFILE LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/textfile_custom';
INSERT INTO textfile_custom VALUES ('1', '"quoted"', '1.50'), ('bad', '中文', 'bad'), ('3', '', ''), ('4', NULL, NULL);

DROP TABLE IF EXISTS textfile_short;
CREATE EXTERNAL TABLE textfile_short (id INT, name STRING)
STORED AS TEXTFILE LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/textfile_short';
INSERT INTO textfile_short VALUES (1, 'old');

SET hive.exec.compress.output=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
DROP TABLE IF EXISTS textfile_gzip;
CREATE EXTERNAL TABLE textfile_gzip (id INT, name STRING)
STORED AS TEXTFILE LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/textfile_gzip';
INSERT INTO textfile_gzip VALUES (1, 'gzip'), (2, NULL);
SET hive.exec.compress.output=false;
RESET mapred.output.compression.codec;

-- Spark's text writer produces a real header on each file.
DROP TABLE IF EXISTS textfile_header;
CREATE TABLE textfile_header USING text
LOCATION 's3://lakelet-e2e/warehouse/hive_db.db/textfile_header'
AS SELECT value FROM (SELECT 0 AS n, 'id|name' AS value UNION ALL SELECT 1, '1|header-a') ORDER BY n;
INSERT INTO textfile_header SELECT value FROM (SELECT 0 AS n, 'id|name' AS value UNION ALL SELECT 1, '2|header-b') ORDER BY n;

SELECT assert_true(count(*) = 6) FROM parquet_modern_nested;
SELECT assert_true(ts = TIMESTAMP '2500-01-01 00:00:00.123456') FROM parquet_int96 WHERE id = 2;
SELECT assert_true(count(*) = 4) FROM textfile_custom;
SELECT assert_true(count(*) = 1) FROM textfile_short;
SELECT assert_true(count(*) = 2 AND count(name) = 1) FROM textfile_gzip;
SELECT assert_true(count(*) = 4) FROM textfile_header;
