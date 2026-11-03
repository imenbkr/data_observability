"""ETL consumer: Kafka -> validated Parquet files on S3, with data-observability metrics.

Delivery is at-least-once: offsets are committed only after a batch is safely in S3.
Invalid records go to a dead-letter topic with the rejection reason in a header.
"""
import calendar
import io
import json
import os
import time
import uuid

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaException, Producer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

BOOT = os.getenv("KAFKA", "kafka1:9092,kafka2:9092,kafka3:9092")
TOPIC = os.getenv("TOPIC", "plant.events")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "plant.events.dlq")
BUCKET = os.getenv("BUCKET", "plant-lake")
BATCH_MAX_RECORDS = int(os.getenv("BATCH_MAX_RECORDS", "50000"))
BATCH_MAX_SECONDS = float(os.getenv("BATCH_MAX_SECONDS", "30"))

FIELDS = ["event_name", "host", "status", "impact_level", "record_ts", "end_ts"]
REQUIRED = set(FIELDS[:5])
SCHEMA = pa.schema([(f, pa.string()) for f in FIELDS])

records = Counter("etl_records_total", "Records loaded to the lake", ["host", "event_name"])
invalid = Counter("etl_invalid_records_total", "Records sent to the DLQ", ["reason"])
lag = Histogram("etl_record_lag_seconds", "Load time minus record_ts",
                buckets=[1, 5, 10, 15, 30, 45, 60, 90, 120, 300, 600, 1800, 3600])
fields = Gauge("etl_fields_per_record", "Field count per record in the last batch", ["stat"])
variants = Gauge("etl_schema_variants", "Distinct field sets in the last batch")
batch_size = Histogram("etl_batch_records", "Records per written batch",
                       buckets=[1000, 5000, 10000, 25000, 50000, 100000])
batch_seconds = Histogram("etl_batch_write_seconds", "Time to write a batch to S3")
last_ok = Gauge("etl_last_success_timestamp_seconds", "Unix time of the last committed batch")

s3 = boto3.client("s3", endpoint_url=os.getenv("S3_ENDPOINT", "http://localstack:4566"),
                  aws_access_key_id="test", aws_secret_access_key="test", region_name="us-east-1")
dlq = Producer({"bootstrap.servers": BOOT, "acks": "all"})

batch, counts, shapes = [], [], set()
started = time.time()


def flush(consumer):
    global batch, counts, shapes, started
    if batch:
        t0 = time.time()
        buf = io.BytesIO()
        pq.write_table(pa.Table.from_pylist(batch, schema=SCHEMA), buf, compression="zstd")
        key = f"events/dt={time.strftime('%Y-%m-%d/hour=%H', time.gmtime())}/{uuid.uuid4()}.parquet"
        s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
        batch_seconds.observe(time.time() - t0)
        batch_size.observe(len(batch))
        fields.labels("min").set(min(counts))
        fields.labels("max").set(max(counts))
        variants.set(len(shapes))
    dlq.flush()
    try:
        consumer.commit(asynchronous=False)
        last_ok.set(time.time())
    except KafkaException as e:
        # nothing to commit, or partitions were just revoked: the next owner re-reads them
        print(f"commit skipped: {e}", flush=True)
    batch, counts, shapes = [], [], set()
    started = time.time()


def reject(msg, reason):
    invalid.labels(reason).inc()
    dlq.produce(DLQ_TOPIC, msg.value(), key=msg.key(), headers={"reason": reason})


def handle(msg):
    try:
        rec = json.loads(msg.value())
    except ValueError:
        return reject(msg, "invalid_json")
    if not isinstance(rec, dict) or REQUIRED - rec.keys():
        return reject(msg, "missing_field")
    try:
        ts = calendar.timegm(time.strptime(rec["record_ts"], "%Y-%m-%dT%H:%M:%SZ"))
    except (TypeError, ValueError):
        return reject(msg, "bad_timestamp")
    lag.observe(time.time() - ts)
    records.labels(rec["host"], rec["event_name"]).inc()
    counts.append(len(rec))
    shapes.add(frozenset(rec))
    batch.append(rec)


def main():
    start_http_server(8000)
    consumer = Consumer({
        "bootstrap.servers": BOOT,
        "group.id": "plant-etl",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "partition.assignment.strategy": "cooperative-sticky",
    })
    consumer.subscribe([TOPIC], on_revoke=lambda c, _parts: flush(c))
    print(f"consuming {TOPIC} -> s3://{BUCKET}", flush=True)
    while True:
        for msg in consumer.consume(num_messages=5000, timeout=1.0):
            if msg.error():
                print(f"consumer error: {msg.error()}", flush=True)
                continue
            handle(msg)
        if len(batch) >= BATCH_MAX_RECORDS or (batch and time.time() - started > BATCH_MAX_SECONDS):
            flush(consumer)


if __name__ == "__main__":
    main()
