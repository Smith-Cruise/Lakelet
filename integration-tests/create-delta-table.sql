DROP TABLE IF EXISTS delta.`s3://lakelet-e2e/warehouse/delta_db.db/orders`;

CREATE TABLE delta.`s3://lakelet-e2e/warehouse/delta_db.db/orders` (
    id INT,
    name STRING,
    amount DECIMAL(10, 2),
    dt STRING
)
USING delta
PARTITIONED BY (dt);

INSERT INTO delta.`s3://lakelet-e2e/warehouse/delta_db.db/orders`
VALUES
    (1, 'alice', CAST(10.50 AS DECIMAL(10, 2)), '2026-06-25'),
    (2, 'bob', CAST(20.25 AS DECIMAL(10, 2)), '2026-06-25'),
    (3, 'carol', CAST(7.00 AS DECIMAL(10, 2)), '2026-06-24'),
    (4, 'dave', CAST(12.30 AS DECIMAL(10, 2)), '2026-06-24');
