CREATE TABLE input_table
(
    user_id STRING,
    item_id STRING,
    behavior STRING
)
WITH ( 'connector' = 'kafka',
    'topic' = 'source',
    'properties.bootstrap.servers' = '10.10.80.70:6667,10.10.80.71:6667,10.10.80.72:6667',
    'scan.startup.mode' = 'earliest-offset',
    'properties.group.id' = 'testGroup005',
    'format' = 'json');

CREATE TABLE output_table
(
    `user_id` STRING,
    `item_id` STRING,
    `behavior` STRING
)
WITH ( 'connector' = 'filesystem',
    'path' = 'hdfs://hadoop1:9000/flink/sink_table',
    'sink.rolling-policy.file-size' = '128MB',
    'sink.rolling-policy.rollover-interval' = '1s',
    'sink.rolling-policy.check-interval' = '1min',
    'sink.parallelism' = '2',
    'format' = 'json');

INSERT INTO output_table
SELECT user_id, item_id, behavior
FROM input_table
WHERE behavior IS NOT NULL;
