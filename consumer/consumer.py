"""Consumes Nouvelair positions from Kafka, checks their quality and exposes metrics
for the five pillars of data observability (volume, freshness, schema, distribution, lineage),
plus the live position of every flight for the Grafana map.
"""
import json
import os
import time

from confluent_kafka import Consumer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

KAFKA = os.getenv("KAFKA", "kafka:9092")
TOPIC = os.getenv("TOPIC", "nouvelair.positions")
ACTIVE_WINDOW = int(os.getenv("ACTIVE_WINDOW_SECONDS", "600"))

REQUIRED = ["hex", "callsign", "lat", "lon", "altitude_ft", "position_ts", "fetched_at"]
FLIGHT_LABELS = ["callsign", "registration", "aircraft_type", "route"]

# Volume
messages = Counter("nouvelair_messages_total", "Messages consumed")
valid = Counter("nouvelair_valid_positions_total", "Messages that passed all checks")
active = Gauge("nouvelair_active_flights", "Flights seen in the active window")
# Freshness
position_age = Histogram("nouvelair_position_age_seconds", "Age of a position when it reaches the consumer",
                         buckets=[5, 10, 20, 30, 45, 60, 90, 120, 180, 300, 600])
stage_latency = Histogram("nouvelair_stage_latency_seconds", "Latency per pipeline stage", ["stage"],
                          buckets=[0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120])
last_message = Gauge("nouvelair_last_message_timestamp_seconds", "Unix time of the last consumed message")
# Schema
invalid = Counter("nouvelair_invalid_messages_total", "Rejected messages", ["reason"])
REASONS = ["invalid_json", "missing_field", "missing_position", "bad_altitude", "out_of_range", "wrong_airline"]
for _r in REASONS:
    invalid.labels(_r)  # export every reason at 0 so ratios work before the first rejection
raw_fields = Gauge("nouvelair_raw_field_count", "Fields per raw ADS-B record in the active window", ["stat"])
schema_variants = Gauge("nouvelair_schema_variants", "Distinct message shapes in the active window")
unknown_route = Counter("nouvelair_unknown_route_total", "Valid positions whose callsign has no known route")
# Distribution
by_destination = Gauge("nouvelair_active_flights_by_destination", "Active flights per destination country",
                       ["destination_country"])
by_type = Gauge("nouvelair_active_flights_by_type", "Active flights per aircraft type", ["aircraft_type"])
altitude = Histogram("nouvelair_altitude_feet", "Reported altitude of airborne aircraft",
                     buckets=[1000, 5000, 10000, 20000, 30000, 35000, 40000])
# Map
lat_g = Gauge("nouvelair_flight_latitude", "Last latitude", FLIGHT_LABELS)
lon_g = Gauge("nouvelair_flight_longitude", "Last longitude", FLIGHT_LABELS)
alt_g = Gauge("nouvelair_flight_altitude_feet", "Last altitude (0 = on ground)", FLIGHT_LABELS)
gs_g = Gauge("nouvelair_flight_ground_speed_knots", "Last ground speed", FLIGHT_LABELS)
track_g = Gauge("nouvelair_flight_track_degrees", "Last track (heading over ground)", FLIGHT_LABELS)

flights = {}  # hex -> {"seen": ts, "labels": tuple, "country": str, "type": str, "fields": int, "shape": frozenset}


def check(msg):
    """Return (record, None) if valid, else (None, reason)."""
    try:
        rec = json.loads(msg.value())
    except ValueError:
        return None, "invalid_json"
    if any(rec.get(k) is None for k in REQUIRED):
        return None, "missing_position" if rec.get("lat") is None or rec.get("lon") is None else "missing_field"
    alt = rec["altitude_ft"]
    if not (alt == "ground" or isinstance(alt, (int, float))):
        return None, "bad_altitude"
    if not (-90 <= rec["lat"] <= 90 and -180 <= rec["lon"] <= 180):
        return None, "out_of_range"
    if not rec["callsign"].startswith("LBT"):
        return None, "wrong_airline"
    return rec, None


def record(rec, kafka_ts):
    now = time.time()
    position_age.observe(now - rec["position_ts"])
    stage_latency.labels("source_to_api").observe(max(0.0, rec["fetched_at"] - rec["position_ts"]))
    stage_latency.labels("api_to_kafka").observe(max(0.0, kafka_ts - rec["fetched_at"]))
    stage_latency.labels("kafka_to_consumer").observe(max(0.0, now - kafka_ts))

    if not rec.get("destination"):
        unknown_route.inc()
    route = f"{rec.get('origin') or '?'} → {rec.get('destination') or '?'}"
    labels = (rec["callsign"], rec.get("registration") or "?", rec.get("aircraft_type") or "?", route)
    old = flights.get(rec["hex"])
    if old and old["labels"] != labels:
        remove_flight(old)  # callsign changed: new leg for this aircraft

    on_ground = rec["altitude_ft"] == "ground"
    alt = 0 if on_ground else rec["altitude_ft"]
    if not on_ground:
        altitude.observe(alt)
    lat_g.labels(*labels).set(rec["lat"])
    lon_g.labels(*labels).set(rec["lon"])
    alt_g.labels(*labels).set(alt)
    gs_g.labels(*labels).set(rec.get("ground_speed_kt") or 0)
    track_g.labels(*labels).set(rec.get("track_deg") or 0)

    flights[rec["hex"]] = {
        "seen": now, "labels": labels,
        "country": rec.get("destination_country") or "Unknown",
        "type": rec.get("aircraft_type") or "Unknown",
        "fields": rec.get("raw_field_count", 0),
        "shape": frozenset(rec),
    }


def remove_flight(f):
    for g in (lat_g, lon_g, alt_g, gs_g, track_g):
        try:
            g.remove(*f["labels"])
        except KeyError:
            pass


def refresh_window():
    now = time.time()
    for hex_, f in list(flights.items()):
        if now - f["seen"] > ACTIVE_WINDOW:
            remove_flight(f)
            del flights[hex_]
    live = list(flights.values())
    active.set(len(live))
    by_destination.clear()
    by_type.clear()
    for f in live:
        by_destination.labels(f["country"]).inc()
        by_type.labels(f["type"]).inc()
    if live:
        raw_fields.labels("min").set(min(f["fields"] for f in live))
        raw_fields.labels("max").set(max(f["fields"] for f in live))
    schema_variants.set(len({f["shape"] for f in live}))


def main():
    start_http_server(8000)
    consumer = Consumer({"bootstrap.servers": KAFKA, "group.id": "nouvelair-quality",
                         "auto.offset.reset": "earliest"})
    consumer.subscribe([TOPIC])
    print(f"consuming {TOPIC}", flush=True)
    next_refresh = 0.0
    while True:
        msg = consumer.poll(1.0)
        if msg is not None and not msg.error():
            messages.inc()
            last_message.set(time.time())
            rec, reason = check(msg)
            if reason:
                invalid.labels(reason).inc()
            else:
                valid.inc()
                record(rec, msg.timestamp()[1] / 1000)
        elif msg is not None:
            print(f"consumer error: {msg.error()}", flush=True)
        if time.time() >= next_refresh:
            refresh_window()
            next_refresh = time.time() + 15


if __name__ == "__main__":
    main()
