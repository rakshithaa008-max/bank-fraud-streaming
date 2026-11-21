-- Kafka transaction source
CREATE TABLE bank_transactions_live_test (
    transaction_id STRING,
    account_id STRING,
    amount DOUBLE,
    currency STRING,
    location STRING,
    `timestamp` STRING,

    event_time AS TO_TIMESTAMP(
        REPLACE(SUBSTRING(`timestamp`, 1, 19), 'T', ' ')
    ),

    WATERMARK FOR event_time AS
        event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'bank-transactions-live-test',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-live-test-v1',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

-- Kafka fraud alert destination
CREATE TABLE fraud_alerts_sink (
    account_id STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    txn_count BIGINT,
    total_amount DOUBLE,
    alert_status STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'fraud-alerts',
    'properties.bootstrap.servers' = 'kafka:29092',
    'format' = 'json'
);
