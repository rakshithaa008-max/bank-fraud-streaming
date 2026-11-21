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
