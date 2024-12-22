CREATE TABLE input_table
(
    user_id STRING,
    item_id STRING,
    behavior STRING
)
WITH ( 'connector' = 'kafka',
    'topic' = 'source',
    'properties.bootstrap.servers' = 'hadoop1:9092,hadoop2:9092,hadoop3:9092',
    'scan.startup.mode' = 'earliest-offset',
    'properties.group.id' = 'testGroup006',
    'format' = 'json');

CREATE TABLE output_table
(
    `user_id` STRING,
    `item_id` STRING,
    `behavior` STRING
)
WITH ( 'connector' = 'print');

INSERT INTO output_table
SELECT user_id, item_id, behavior
FROM input_table
WHERE behavior IS NOT NULL;
