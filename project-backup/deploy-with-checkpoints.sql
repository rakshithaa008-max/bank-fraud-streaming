SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '30 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.storage' = 'filesystem';
SET 'execution.checkpointing.dir' = 'file:///opt/flink/checkpoints';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
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
-- Rule 1: Frequent transactions in a 5-minute window

INSERT INTO fraud_alerts_sink
SELECT
    account_id,
    window_start,
    window_end,
    COUNT(*) AS txn_count,
    SUM(amount) AS total_amount,
    'FREQUENT_TRANSACTION_ALERT' AS alert_status
FROM TABLE(
    TUMBLE(
        TABLE bank_transactions_live_test,
        DESCRIPTOR(event_time),
        INTERVAL '5' MINUTES
    )
)
WHERE account_id = 'ACC888'
GROUP BY
    account_id,
    window_start,
    window_end
HAVING COUNT(*) >= 3;
-- Rule 2: Total transactions >= 2000 in 5 minutes

INSERT INTO fraud_alerts_sink
SELECT
    account_id,
    window_start,
    window_end,
    COUNT(*) AS txn_count,
    SUM(amount) AS total_amount,
    'HIGH_VALUE_WINDOW_ALERT' AS alert_status
FROM TABLE(
    TUMBLE(
        TABLE bank_transactions_live_test,
        DESCRIPTOR(event_time),
        INTERVAL '5' MINUTES
    )
)
GROUP BY
    account_id,
    window_start,
    window_end
HAVING SUM(amount) >= 2000;
