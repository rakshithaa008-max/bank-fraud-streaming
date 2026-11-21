
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "bank-transactions",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="fraud-detection-v1",
    value_deserializer=lambda value: value.decode("utf-8")
)

print("Fraud Detection Consumer Started...")
print("Waiting for bank transactions...")

for message in consumer:
    try:
        transaction = json.loads(message.value)

        transaction_id = transaction.get("transaction_id")
        amount = float(transaction.get("amount", 0))

        print(f"\nTransaction: {transaction_id}")
        print(f"Amount: GBP {amount:.2f}")

        if amount > 2000:
            print("ALERT: HIGH_VALUE_ALERT")
        else:
            print("Status: NORMAL")

    except (json.JSONDecodeError, ValueError, TypeError) as error:
        print(f"Invalid transaction skipped: {error}")

