"""Synthetic industrial-plant event producer.

Emits JSON events for 50 pumps at a target rate, with built-in incidents so every
data-observability pillar has something to show:
  - volume:       every INCIDENT_EVERY_MIN minutes, the rate drops to 25% for 3 minutes
  - schema:       a fraction SCHEMA_BREAK_RATE of records lose the "host" field
  - distribution: 5 "hot" pumps produce ~half of all events
"""
import json
import os
import random
import time

from confluent_kafka import Producer
from prometheus_client import Counter, start_http_server

BOOT = os.getenv("KAFKA", "kafka1:9092,kafka2:9092,kafka3:9092")
TOPIC = os.getenv("TOPIC", "plant.events")
RATE = int(os.getenv("EVENTS_PER_SEC", "5000"))
INCIDENT_EVERY_MIN = int(os.getenv("INCIDENT_EVERY_MIN", "30"))
SCHEMA_BREAK_RATE = float(os.getenv("SCHEMA_BREAK_RATE", "0.001"))

HOSTS = [f"pump-{i:02d}" for i in range(1, 51)]
WEIGHTS = [10 if i < 5 else 1 for i in range(len(HOSTS))]
EVENTS = ["Low pump pressure", "High temperature", "Vibration alert"]

sent = Counter("producer_records_sent_total", "Records acknowledged by Kafka")
failed = Counter("producer_records_failed_total", "Records Kafka failed to acknowledge")


def on_delivery(err, _msg):
    (failed if err else sent).inc()


def in_incident(now):
    return INCIDENT_EVERY_MIN > 0 and int(now // 60) % INCIDENT_EVERY_MIN < 3


def make_record(now):
    rec = {
        "event_name": random.choices(EVENTS, [8, 1, 1])[0],
        "host": random.choices(HOSTS, WEIGHTS)[0],
        "status": random.choice(["OPEN", "CLOSED"]),
        "impact_level": "INFRASTRUCTURE",
        # the source system timestamps the event up to 30 s before we publish it
        "record_ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now - random.uniform(0, 30))),
    }
    if rec["status"] == "CLOSED":
        rec["end_ts"] = rec["record_ts"]
    if random.random() < SCHEMA_BREAK_RATE:
        del rec["host"]
    return rec


def main():
    start_http_server(8001)
    p = Producer({
        "bootstrap.servers": BOOT,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "linger.ms": 20,
        "batch.size": 262144,
    })
    print(f"producing ~{RATE} events/s to {TOPIC}", flush=True)
    while True:
        tick = time.time()
        rate = RATE // 4 if in_incident(tick) else RATE
        for _ in range(rate):
            rec = make_record(tick)
            p.produce(TOPIC, json.dumps(rec).encode(), key=rec.get("host", "unknown"),
                      on_delivery=on_delivery)
            p.poll(0)
        p.poll(0)
        time.sleep(max(0.0, 1.0 - (time.time() - tick)))


if __name__ == "__main__":
    main()
