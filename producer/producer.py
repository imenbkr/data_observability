"""Polls live ADS-B data from adsb.lol and publishes flights to and from Tunisia to Kafka.

Every POLL_SECONDS it queries areas covering Tunisia, the Mediterranean and the main
European markets. It keeps an aircraft when its callsign is a known route to, from or
within Tunisia (data/routes.csv, any airline), or when it belongs to a Tunisian airline.
Each kept aircraft is enriched with its route and sent as one message.
"""
import csv
import json
import os
import time

import requests
from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

KAFKA = os.getenv("KAFKA", "kafka:9092")
TOPIC = os.getenv("TOPIC", "tunisia.flights")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))
API = "https://api.adsb.lol/v2/point/{lat}/{lon}/{radius}"
USER_AGENT = os.getenv("USER_AGENT", "tunisia-flights-data-observability-demo")

# Tunisian airlines are kept even when their flight number is missing from the route list
TUNISIAN_AIRLINES = {"TAR": "Tunisair", "LBT": "Nouvelair", "TUX": "Tunisair Express"}

# (name, lat, lon); each area is a 250 NM radius circle
AREAS = [
    ("Tunisia North", 36.5, 10.0),
    ("Tunisia South", 34.0, 10.5),
    ("Libya & Malta", 34.0, 15.5),
    ("Algeria", 36.0, 4.0),
    ("Central Mediterranean", 40.5, 8.5),
    ("Southern France", 44.0, 4.5),
    ("Northern France & Benelux", 49.5, 3.0),
    ("United Kingdom", 52.5, -1.5),
    ("Western Germany", 50.5, 8.0),
    ("Eastern Germany & Poland", 52.0, 15.0),
]

api_requests = Counter("adsb_api_requests_total", "Requests to adsb.lol", ["area", "status"])
api_latency = Histogram("adsb_api_request_seconds", "adsb.lol response time", ["area"])
aircraft_seen = Gauge("adsb_aircraft_in_areas", "All aircraft returned by the last poll")
kept = Gauge("producer_tunisia_aircraft", "Aircraft on a Tunisia flight found in the last poll")
sent = Counter("producer_messages_sent_total", "Messages acknowledged by Kafka")
failed = Counter("producer_messages_failed_total", "Messages Kafka failed to acknowledge")
last_poll = Gauge("producer_last_poll_timestamp_seconds", "Unix time of the last completed poll")


def load_routes():
    with open("/app/data/routes.csv", newline="") as f:
        return {r["callsign"]: r for r in csv.DictReader(f)}


class RateLimited(Exception):
    def __init__(self, retry_after):
        super().__init__(f"rate limited, retry after {retry_after}s")
        self.retry_after = retry_after


def fetch_area(session, name, lat, lon):
    t0 = time.time()
    try:
        resp = session.get(API.format(lat=lat, lon=lon, radius=250), timeout=20)
        api_latency.labels(name).observe(time.time() - t0)
        api_requests.labels(name, str(resp.status_code)).inc()
        if resp.status_code == 429:
            raise RateLimited(int(resp.headers.get("Retry-After", "30")))
        resp.raise_for_status()
        body = resp.json()
        return body.get("now", time.time() * 1000) / 1000, time.time(), body.get("ac", [])
    except (requests.RequestException, ValueError) as e:
        api_requests.labels(name, "error").inc()
        print(f"{name}: {e}", flush=True)
        return None, None, []


def is_tunisia_flight(callsign, routes):
    return callsign in routes or callsign[:3] in TUNISIAN_AIRLINES


def coord(route, key):
    return float(route[key]) if route.get(key) else None


def to_message(ac, api_now, fetched_at, area, routes):
    callsign = (ac.get("flight") or "").strip()
    route = routes.get(callsign, {})
    seen_pos = ac.get("seen_pos")
    return {
        "hex": ac.get("hex"),
        "callsign": callsign,
        "airline_code": callsign[:3],
        "airline": route.get("airline") or TUNISIAN_AIRLINES.get(callsign[:3]),
        "direction": route.get("direction"),
        "registration": ac.get("r"),
        "aircraft_type": ac.get("t"),
        "lat": ac.get("lat"),
        "lon": ac.get("lon"),
        "altitude_ft": ac.get("alt_baro"),  # int, or the string "ground" - kept as-is on purpose
        "ground_speed_kt": ac.get("gs"),
        "track_deg": ac.get("track"),
        "vertical_rate_fpm": ac.get("baro_rate"),
        "squawk": ac.get("squawk"),
        "origin": route.get("origin"),
        "origin_city": route.get("origin_city"),
        "origin_country": route.get("origin_country"),
        "destination": route.get("destination"),
        "destination_city": route.get("destination_city"),
        "destination_country": route.get("destination_country"),
        "destination_lat": coord(route, "destination_lat"),
        "destination_lon": coord(route, "destination_lon"),
        "position_ts": api_now - seen_pos if seen_pos is not None else None,
        "fetched_at": fetched_at,
        "source": "adsb.lol",
        "source_area": area,
        "raw_field_count": len(ac),
    }


def on_delivery(err, _msg):
    (failed if err else sent).inc()


def main():
    start_http_server(8001)
    routes = load_routes()
    producer = Producer({"bootstrap.servers": KAFKA, "acks": "all", "linger.ms": 50})
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    spacing = POLL_SECONDS / len(AREAS)
    print(f"tracking flights to/from Tunisia in {len(AREAS)} areas every {POLL_SECONDS}s "
          f"({len(routes)} known routes)", flush=True)

    while True:
        started = time.time()
        latest, total = {}, 0
        for name, lat, lon in AREAS:
            try:
                api_now, fetched_at, aircraft = fetch_area(session, name, lat, lon)
            except RateLimited as e:
                print(f"{name}: {e}", flush=True)
                time.sleep(e.retry_after)
                continue
            total += len(aircraft)
            for ac in aircraft:
                if not is_tunisia_flight((ac.get("flight") or "").strip(), routes):
                    continue
                # areas overlap: keep the freshest position per aircraft
                prev = latest.get(ac.get("hex"))
                if prev is None or ac.get("seen_pos", 1e9) < prev[0].get("seen_pos", 1e9):
                    latest[ac.get("hex")] = (ac, api_now, fetched_at, name)
            time.sleep(spacing)  # spread requests evenly over the poll interval

        for ac, api_now, fetched_at, area in latest.values():
            msg = to_message(ac, api_now, fetched_at, area, routes)
            producer.produce(TOPIC, json.dumps(msg).encode(), key=msg["hex"], on_delivery=on_delivery)
        producer.flush(10)

        aircraft_seen.set(total)
        kept.set(len(latest))
        last_poll.set(time.time())
        print(f"poll: {total} aircraft, {len(latest)} Tunisia flights: "
              f"{', '.join(sorted(m[0]['flight'].strip() for m in latest.values()))}", flush=True)
        time.sleep(max(0.0, POLL_SECONDS - (time.time() - started)))


if __name__ == "__main__":
    main()
