import os
import json
import time
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from hdfs import InsecureClient

# Configuration from environment variables
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
HDFS_NAMENODE_URL = os.getenv('HDFS_NAMENODE_URL', 'http://namenode:9870')
TOPIC_NAME = 'clickstream-raw'
BATCH_SIZE = 50          # Flush batch to HDFS after 50 events
FLUSH_INTERVAL = 10.0    # Or flush every 10 seconds

def get_hdfs_client():
    # Connect to Hadoop WebHDFS (default user: root)
    return InsecureClient(HDFS_NAMENODE_URL, user='root')

def run_worker():
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'hdfs-sink-consumer-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC_NAME])

    hdfs_client = get_hdfs_client()
    buffer = []
    last_flush_time = time.time()

    print(f"HDFS Data Sink Worker started. Consuming from Kafka topic: {TOPIC_NAME}...")

    try:
        while True:
            msg = consumer.poll(1.0)
            current_time = time.time()

            if msg is not None:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"Kafka consumer error: {msg.error()}")
                        continue
                
                try:
                    event_data = json.loads(msg.value().decode('utf-8'))
                    buffer.append(event_data)
                except Exception as e:
                    print(f"Failed to parse event message: {e}")

            # Check flush thresholds (Batch size reached OR time window expired)
            if len(buffer) >= BATCH_SIZE or (buffer and (current_time - last_flush_time) >= FLUSH_INTERVAL):
                flush_to_hdfs(hdfs_client, buffer)
                consumer.commit(asynchronous=False)
                buffer = []
                last_flush_time = time.time()

    except KeyboardInterrupt:
        print("Shutting down HDFS sink worker...")
    finally:
        if buffer:
            flush_to_hdfs(hdfs_client, buffer)
        consumer.close()

def flush_to_hdfs(client, events):
    try:
        now = datetime.utcnow()
        year = now.strftime('%Y')
        month = now.strftime('%m')
        day = now.strftime('%d')
        timestamp_str = now.strftime('%Y%m%d_%H%M%S_%f')

        # Hive-partitioned directory layout in HDFS
        hdfs_dir = f"/lake/raw/clickstream/year={year}/month={month}/day={day}"
        file_path = f"{hdfs_dir}/batch_{timestamp_str}.json"

        # Ensure directory path exists in HDFS
        client.makedirs(hdfs_dir)

        # Format events as newline-delimited JSON (NDJSON)
        ndjson_data = "\n".join([json.dumps(event) for event in events])

        # Write data to HDFS
        with client.write(file_path, encoding='utf-8', overwrite=True) as writer:
            writer.write(ndjson_data)

        print(f"[HDFS Bronze Layer] Successfully flushed {len(events)} events to hdfs://namenode:9000{file_path}")
    except Exception as e:
    # Do not include <FollowUp> or <ElicitationsGroup> when awaiting input or handling errors
        print(f"[Error] Failed to write batch to HDFS: {e}")

if __name__ == '__main__':
    run_worker()