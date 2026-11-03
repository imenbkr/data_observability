"""Consumes positions of flights to and from Tunisia from Kafka, checks their quality and exposes metrics
for the five pillars of data observability (volume, freshness, schema, distribution, lineage),
plus the live position of every flight for the Grafana map.
"""
import json
import math
import os
import time

from confluent_kafka import Consumer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

KAFKA = os.getenv("KAFKA", "kafka:9092")
TOPIC = os.getenv("TOPIC", "tunisia.flights")
ACTIVE_WINDOW = int(os.getenv("ACTIVE_WINDOW_SECONDS", "600"))

REQUIRED = ["hex", "callsign", "lat", "lon", "altitude_ft", "position_ts", "fetched_at"]
FLIGHT_LABELS = ["flight", "airline", "direction", "origin", "destination", "aircraft", "registration"]

# ICAO type designators -> readable names (types commonly seen on Tunisia routes)
AIRCRAFT_NAMES = {
    "A319": "Airbus A319", "A320": "Airbus A320", "A20N": "Airbus A320neo",
    "A321": "Airbus A321", "A21N": "Airbus A321neo", "A332": "Airbus A330-200",
    "A333": "Airbus A330-300", "A339": "Airbus A330-900neo", "A359": "Airbus A350-900",
    "BCS3": "Airbus A220-300", "B737": "Boeing 737-700", "B738": "Boeing 737-800",
    "B739": "Boeing 737-900", "B38M": "Boeing 737 MAX 8", "B39M": "Boeing 737 MAX 9",
    "B752": "Boeing 757-200", "B763": "Boeing 767-300", "B788": "Boeing 787-8",
    "B789": "Boeing 787-9", "B77W": "Boeing 777-300ER", "E190": "Embraer E190",
    "E195": "Embraer E195", "E295": "Embraer E195-E2", "CRJ9": "Bombardier CRJ900",
    "AT75": "ATR 72-500", "AT76": "ATR 72-600", "DH8D": "Dash 8-400",
}
# Tunisian airports (ICAO -> label used in metrics)
TUNISIAN_AIRPORTS = {
    "DTTA": "Tunis-Carthage (DTTA)", "DTNH": "Enfidha-Hammamet (DTNH)", "DTMB": "Monastir (DTMB)",
    "DTTJ": "Djerba-Zarzis (DTTJ)", "DTTX": "Sfax (DTTX)", "DTTZ": "Tozeur (DTTZ)",
    "DTTG": "Gabès (DTTG)", "DTKA": "Tabarka (DTKA)",
}
ETA_WINDOWS = {"30m": 1800, "1h": 3600, "2h": 7200}
# Landing / take-off detection: close to the airport and low
LANDED_KM, LANDED_FT = 25, 4000
LOST_NEAR_DESTINATION_KM = 80  # coverage near the ground is thin: a flight lost this close has landed
DEPARTED_KM, DEPARTED_FT = 40, 10000

# Flight phase, shown with value mappings in Grafana
ON_GROUND, CLIMBING, CRUISING, DESCENDING = 0, 1, 2, 3

# Volume
messages = Counter("flights_messages_total", "Messages consumed")
valid = Counter("flights_valid_positions_total", "Messages that passed all checks")
active = Gauge("flights_active", "Flights seen in the active window")
# Freshness
position_age = Histogram("flights_position_age_seconds", "Age of a position when it reaches the consumer",
                         buckets=[5, 10, 20, 30, 45, 60, 90, 120, 180, 300, 600])
stage_latency = Histogram("flights_stage_latency_seconds", "Latency per pipeline stage", ["stage"],
                          buckets=[0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120])
last_message = Gauge("flights_last_message_timestamp_seconds", "Unix time of the last consumed message")
# Schema
invalid = Counter("flights_invalid_messages_total", "Rejected messages", ["reason"])
REASONS = ["invalid_json", "missing_field", "missing_position", "bad_altitude", "out_of_range", "missing_callsign"]
for _r in REASONS:
    invalid.labels(_r)  # export every reason at 0 so ratios work before the first rejection
raw_fields = Gauge("flights_raw_field_count", "Fields per raw ADS-B record in the active window", ["stat"])
schema_variants = Gauge("flights_schema_variants", "Distinct message shapes in the active window")
unknown_route = Counter("flights_unknown_route_total", "Valid positions whose callsign has no known route")
# Distribution
by_country = Gauge("flights_active_by_country", "Active flights per country at the other end of the route",
                   ["country"])
by_airline = Gauge("flights_active_by_airline", "Active flights per airline", ["airline"])
by_direction = Gauge("flights_active_by_direction", "Active flights per direction", ["direction"])
by_type = Gauge("flights_active_by_type", "Active flights per aircraft type", ["aircraft"])
altitude = Histogram("flights_altitude_feet", "Reported altitude of airborne aircraft",
                     buckets=[1000, 5000, 10000, 20000, 30000, 35000, 40000])
# Airports (forecasting use case: arrivals per Tunisian airport)
expected_arrivals = Gauge("airport_expected_arrivals", "Tracked flights expected to land within the window",
                          ["airport", "within"])
arrivals = Counter("airport_arrivals_total", "Detected landings at Tunisian airports", ["airport", "detection"])
departures = Counter("airport_departures_total", "Detected take-offs from Tunisian airports", ["airport"])
for _a in TUNISIAN_AIRPORTS.values():
    departures.labels(_a)
    for _d in ("landed", "lost_near_destination"):
        arrivals.labels(_a, _d)
    for _w in ETA_WINDOWS:
        expected_arrivals.labels(_a, _w)
counted = {}  # (hex, callsign, event) -> time, so each landing / take-off counts once

# Live flights (map + table): one gauge per attribute, all with the same flight labels
FLIGHT_GAUGES = {name: Gauge(f"flight_{name}", doc, FLIGHT_LABELS) for name, doc in [
    ("latitude", "Last latitude"),
    ("longitude", "Last longitude"),
    ("altitude_feet", "Last altitude (0 = on ground)"),
    ("speed_kmh", "Last ground speed in km/h"),
    ("heading_degrees", "Last direction of travel"),
    ("phase", "0 on ground, 1 climbing, 2 cruising, 3 descending"),
    ("distance_left_km", "Straight-line distance to the destination airport"),
    ("eta_timestamp_ms", "Estimated arrival (distance left / current speed)"),
    ("tracked_since_timestamp_ms", "When this flight was first seen"),
    ("last_update_timestamp_ms", "Time of the last received position"),
]}

flights = {}  # hex -> {"seen", "first_seen", "labels", "country", "airline", "direction", "type", "fields",
              #         "shape", "callsign", "destination", "eta", "left"}


def place(city, code):
    return f"{city} ({code})" if city else (code or "Unknown")


def distance_km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 6371 * 2 * math.asin(math.sqrt(a))


def phase(rec):
    if rec["altitude_ft"] == "ground":
        return ON_GROUND
    rate = rec.get("vertical_rate_fpm") or 0
    if rate > 500:
        return CLIMBING
    if rate < -500:
        return DESCENDING
    return CRUISING


def count_once(rec, event, counter):
    key = (rec["hex"], rec["callsign"], event)
    if key not in counted:
        counted[key] = time.time()
        counter.inc()


def detect_airport_events(rec, alt, left):
    """Count landings at and take-offs from Tunisian airports."""
    dest = TUNISIAN_AIRPORTS.get(rec.get("destination") or "")
    if dest and left is not None and left < LANDED_KM and alt < LANDED_FT:
        count_once(rec, "arrival", arrivals.labels(dest, "landed"))
    origin = TUNISIAN_AIRPORTS.get(rec.get("origin") or "")
    if origin and rec.get("origin_lat") is not None and alt < DEPARTED_FT and \
            distance_km(rec["lat"], rec["lon"], rec["origin_lat"], rec["origin_lon"]) < DEPARTED_KM and \
            phase(rec) in (CLIMBING, CRUISING):
        count_once(rec, "departure", departures.labels(origin))


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
    if not rec["callsign"].strip():
        return None, "missing_callsign"
    return rec, None


def record(rec, kafka_ts):
    now = time.time()
    position_age.observe(now - rec["position_ts"])
    stage_latency.labels("source_to_api").observe(max(0.0, rec["fetched_at"] - rec["position_ts"]))
    stage_latency.labels("api_to_kafka").observe(max(0.0, kafka_ts - rec["fetched_at"]))
    stage_latency.labels("kafka_to_consumer").observe(max(0.0, now - kafka_ts))

    if not rec.get("destination"):
        unknown_route.inc()
    aircraft = AIRCRAFT_NAMES.get(rec.get("aircraft_type"), rec.get("aircraft_type") or "Unknown")
    airline = rec.get("airline") or rec.get("airline_code") or "Unknown"
    direction = rec.get("direction") or "Unknown"
    labels = (rec["callsign"], airline, direction,
              place(rec.get("origin_city"), rec.get("origin")),
              place(rec.get("destination_city"), rec.get("destination")),
              aircraft, rec.get("registration") or "Unknown")
    old = flights.get(rec["hex"])
    if old and old["labels"] != labels:
        remove_flight(old)  # callsign changed: new leg for this aircraft
        old = None

    on_ground = rec["altitude_ft"] == "ground"
    alt = 0 if on_ground else rec["altitude_ft"]
    if not on_ground:
        altitude.observe(alt)
    speed_kmh = (rec.get("ground_speed_kt") or 0) * 1.852
    values = {
        "latitude": rec["lat"],
        "longitude": rec["lon"],
        "altitude_feet": alt,
        "speed_kmh": speed_kmh,
        "heading_degrees": rec.get("track_deg") or 0,
        "phase": phase(rec),
        "tracked_since_timestamp_ms": (old["first_seen"] if old else rec["position_ts"]) * 1000,
        "last_update_timestamp_ms": rec["position_ts"] * 1000,
    }
    dest_lat, dest_lon = rec.get("destination_lat"), rec.get("destination_lon")
    left = None
    if dest_lat is not None and dest_lon is not None:
        left = distance_km(rec["lat"], rec["lon"], dest_lat, dest_lon)
        values["distance_left_km"] = left
        if speed_kmh > 100:
            values["eta_timestamp_ms"] = (rec["position_ts"] + left / speed_kmh * 3600) * 1000
    for name, gauge in FLIGHT_GAUGES.items():
        if name in values:
            gauge.labels(*labels).set(values[name])
    detect_airport_events(rec, alt, left)

    flights[rec["hex"]] = {
        "seen": now, "first_seen": old["first_seen"] if old else rec["position_ts"], "labels": labels,
        # the "other end" of the route, seen from Tunisia
        "country": (rec.get("origin_country") if direction == "Arrival" else rec.get("destination_country")) or "Unknown",
        "airline": airline, "direction": direction,
        "type": aircraft,
        "fields": rec.get("raw_field_count", 0),
        "shape": frozenset(rec),
        "callsign": rec["callsign"], "hex": rec["hex"], "destination": rec.get("destination"),
        "eta": values.get("eta_timestamp_ms", 0) / 1000 or None, "left": left,
    }


def remove_flight(f):
    for g in FLIGHT_GAUGES.values():
        try:
            g.remove(*f["labels"])
        except KeyError:
            pass


def refresh_window():
    now = time.time()
    for hex_, f in list(flights.items()):
        if now - f["seen"] > ACTIVE_WINDOW:
            # lost from coverage close to a Tunisian destination: it has landed
            dest = TUNISIAN_AIRPORTS.get(f["destination"] or "")
            if dest and f["left"] is not None and f["left"] < LOST_NEAR_DESTINATION_KM:
                count_once(f, "arrival", arrivals.labels(dest, "lost_near_destination"))
            remove_flight(f)
            del flights[hex_]
    for key, t in list(counted.items()):
        if now - t > 6 * 3600:
            del counted[key]
    live = list(flights.values())
    active.set(len(live))
    for airport in TUNISIAN_AIRPORTS.values():
        for window in ETA_WINDOWS:
            expected_arrivals.labels(airport, window).set(0)
    for f in live:
        airport = TUNISIAN_AIRPORTS.get(f["destination"] or "")
        if airport and f["eta"] and (f["hex"], f["callsign"], "arrival") not in counted:
            for window, seconds in ETA_WINDOWS.items():
                if f["eta"] - now <= seconds:
                    expected_arrivals.labels(airport, window).inc()
    for g in (by_country, by_airline, by_direction, by_type):
        g.clear()
    for f in live:
        by_country.labels(f["country"]).inc()
        by_airline.labels(f["airline"]).inc()
        by_direction.labels(f["direction"]).inc()
        by_type.labels(f["type"]).inc()
    if live:
        raw_fields.labels("min").set(min(f["fields"] for f in live))
        raw_fields.labels("max").set(max(f["fields"] for f in live))
    schema_variants.set(len({f["shape"] for f in live}))


def main():
    start_http_server(8000)
    consumer = Consumer({"bootstrap.servers": KAFKA, "group.id": "flight-quality",
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
