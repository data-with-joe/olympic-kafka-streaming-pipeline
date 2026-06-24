from kafka import KafkaProducer
import json
import time
import pandas as pd

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load the dataset
df = pd.read_csv('athlete_events.csv')

print(f"Loaded {len(df)} rows. Starting to stream...")

count = 0
for _, row in df.iterrows():
    event = row.where(pd.notnull(row), None).to_dict()  # handle NaN values
    producer.send('olympic-events', value=event)
    count += 1

    if count % 500 == 0:
        print(f"Sent {count} events so far...")

    time.sleep(0.01)  # small delay so it feels like a stream, not a dump

producer.flush()
print(f"Done. Sent {count} events total.")