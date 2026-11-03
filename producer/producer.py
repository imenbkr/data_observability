"""Polls live ADS-B data from adsb.lol and publishes Nouvelair flights to Kafka.

Every POLL_SECONDS it queries a few areas covering Tunisia and Nouvelair's main European
markets, keeps aircraft whose callsign starts with LBT (Nouvelair's ICAO code), enriches
them with the route from data/routes.csv and sends one message per aircraft.
"""
import csv
import json
import os
import time

import requests
from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

KAFKA = os.getenv("KAFKA", "kafka:9092")
TOPIC = os.getenv("TOPIC", "nouvelair.positions")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))
AIRLINE_PREFIX = os.getenv("AIRLINE_PREFIX", "LBT")
API = "https://api.adsb.lol/v2/point/{lat}/{lon}/{radius}"
USER_AGENT = os.getenv("USER_AGENT", "nouvelair-data-observability-demo")

# (name, lat, lon); each area is a 250 NM radius circle
AREAS = [
    ("Tunisia North", 36.5, 10.0),
    ("Tunisia South", 34.0, 10.5),
    ("Central Mediterranean", 40.5, 8.5),
    ("Southern France", 44.0, 4.5),
    ("Northern France & Benelux", 49.5, 3.0),
    ("Western Germany", 50.5, 8.0),
    ("Eastern Germany & Poland", 52.0, 15.0),
]

api_requests = Counter("adsb_api_requests_total", "Requests to adsb.lol", ["area", "status"])
api_latency = Histogram("adsb_api_request_seconds", "adsb.lol response time", ["area"])
aircraft_seen = Gauge("adsb_aircraft_in_areas", "All aircraft returned by the last poll")
airline_seen = Gauge("producer_airline_aircraft", "Airline aircraft found in the last poll")
sent = Counter("producer_messages_sent_total", "Messages acknowledged by Kafka")
failed = Counter("producer_messages_failed_total", "Messages Kafka failed to acknowledge")
last_poll = Gauge("producer_last_poll_timestamp_seconds", "Unix time of the last completed poll")


def load_routes():
    with open("/app/data/routes.csv", newline="") as f:
        return {r["callsign"]: r for r in csv.DictReader(f)}


def fetch_area(session, name, lat, lon):
    t0 = time.time()
    try:
        resp = session.get(API.format(lat=lat, lon=lon, radius=250), timeout=20)
        api_latency.labels(name).observe(time.time() - t0)
        api_requests.labels(name, str(resp.status_code)).inc()
        resp.raise_for_status()
        body = resp.json()
        return body.get("now", time.time() * 1000) / 1000, time.time(), body.get("ac", [])
    except (requests.RequestException, ValueError) as e:
        api_requests.labels(name, "error").inc()
        print(f"{name}: {e}", flush=True)
        return None, None, []


def to_message(ac, api_now, fetched_at, area, routes):
    callsign = (ac.get("flight") or "").strip()
    route = routes.get(callsign, {})
    alt = ac.get("alt_baro")
    seen_pos = ac.get("seen_pos")
    return {
        "hex": ac.get("hex"),
        "callsign": callsign,
        "registration": ac.get("r"),
        "aircraft_type": ac.get("t"),
        "lat": ac.get("lat"),
        "lon": ac.get("lon"),
        "altitude_ft": alt,  # int, or the string "ground" - kept as-is on purpose
        "ground_speed_kt": ac.get("gs"),
        "track_deg": ac.get("track"),
        "vertical_rate_fpm": ac.get("baro_rate"),
        "squawk": ac.get("squawk"),
        "origin": route.get("origin"),
        "origin_city": route.get("origin_city"),
        "destination": route.get("destination"),
        "destination_city": route.get("destination_city"),
        "destination_country": route.get("destination_country"),
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
    print(f"tracking {AIRLINE_PREFIX}* flights in {len(AREAS)} areas every {POLL_SECONDS}s "
          f"({len(routes)} known routes)", flush=True)

    while True:
        started = time.time()
        latest, total = {}, 0
        for name, lat, lon in AREAS:
            api_now, fetched_at, aircraft = fetch_area(session, name, lat, lon)
            total += len(aircraft)
            for ac in aircraft:
                if not (ac.get("flight") or "").startswith(AIRLINE_PREFIX):
                    continue
                # areas overlap: keep the freshest position per aircraft
                prev = latest.get(ac.get("hex"))
                if prev is None or ac.get("seen_pos", 1e9) < prev[0].get("seen_pos", 1e9):
                    latest[ac.get("hex")] = (ac, api_now, fetched_at, name)
            time.sleep(1)  # be polite to a free API

        for ac, api_now, fetched_at, area in latest.values():
            msg = to_message(ac, api_now, fetched_at, area, routes)
            producer.produce(TOPIC, json.dumps(msg).encode(), key=msg["hex"], on_delivery=on_delivery)
        producer.flush(10)

        aircraft_seen.set(total)
        airline_seen.set(len(latest))
        last_poll.set(time.time())
        print(f"poll: {total} aircraft, {len(latest)} {AIRLINE_PREFIX} flights: "
              f"{', '.join(sorted(m[0]['flight'].strip() for m in latest.values()))}", flush=True)
        time.sleep(max(0.0, POLL_SECONDS - (time.time() - started)))


if __name__ == "__main__":
    main()
