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
